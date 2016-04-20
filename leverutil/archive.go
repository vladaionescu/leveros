package leverutil

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
)

var bufferedReaderPool = &sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 32*1024)
	},
}

var bufferedWriterPool = &sync.Pool{
	New: func() interface{} {
		return bufio.NewWriterSize(nil, 32*1024)
	},
}

// Tar archives a source directory to a .tar.gz archive.
func Tar(writer io.Writer, sourceDir string) error {
	writerBuffer := bufferedWriterPool.Get().(*bufio.Writer)
	writerBuffer.Reset(writer)
	gzipStream := gzip.NewWriter(writerBuffer)

	tarWriter := tar.NewWriter(gzipStream)
	buffer := bufferedWriterPool.Get().(*bufio.Writer)

	success := false
	defer func() {
		if !success {
			tarWriter.Close()
			gzipStream.Close()
			bufferedWriterPool.Put(buffer)
		}
	}()

	info, err := os.Lstat(sourceDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("Tar source must be a directory")
	}

	filepath.Walk(
		sourceDir,
		func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			relativePath, err := filepath.Rel(sourceDir, filePath)
			if err != nil {
				return err
			}
			if relativePath == "." && info.IsDir() {
				// Skip the source dir itself.
				return nil
			}

			return packFile(tarWriter, buffer, filePath, relativePath, info)
		})

	err = tarWriter.Close()
	if err != nil {
		return err
	}
	err = gzipStream.Close()
	if err != nil {
		return err
	}
	err = writerBuffer.Flush()
	if err != nil {
		return err
	}
	bufferedWriterPool.Put(buffer)

	success = true
	return nil
}

func packFile(
	writer *tar.Writer, buffer *bufio.Writer, filePath string,
	relativePath string, info os.FileInfo) error {
	link := ""
	var err error
	if info.Mode()&os.ModeSymlink != 0 {
		link, err = os.Readlink(filePath)
		if err != nil {
			return err
		}
	}

	header, err := tar.FileInfoHeader(info, link)
	if err != nil {
		return err
	}

	// Build up the name of the file in the tar archive.
	header.Name = relativePath
	if runtime.GOOS == "windows" {
		if strings.Contains(header.Name, "/") {
			return fmt.Errorf("Forward slash in path when running on windows")
		}
		header.Name = strings.Replace(header.Name, string(os.PathSeparator), "/", -1)
	}
	if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
		header.Name += "/"
	}

	err = writer.WriteHeader(header)
	if err != nil {
		return err
	}

	if header.Typeflag == tar.TypeReg {
		// Regular file. Write its contents.
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}

		buffer.Reset(writer)
		_, err = io.Copy(buffer, file)
		if err != nil {
			file.Close()
			return err
		}
		err = file.Close()
		if err != nil {
			return err
		}
		err = buffer.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

// Untar extracts a .tar.gz archive to the provided target directory.
func Untar(reader io.Reader, targetDir string) error {
	gzipStream, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	tarStream := tar.NewReader(gzipStream)

	readerBuffer := bufferedReaderPool.Get().(*bufio.Reader)
	defer bufferedReaderPool.Put(readerBuffer)

	for {
		header, err := tarStream.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		header.Name = filepath.Clean(header.Name)
		// For non-directories, create all necessary parent dirs.
		if !strings.HasSuffix(header.Name, string(os.PathSeparator)) {
			parent := filepath.Dir(header.Name)
			parentPath := filepath.Join(targetDir, parent)
			_, err = os.Lstat(parentPath)
			if err != nil && os.IsNotExist(err) {
				err = os.MkdirAll(parentPath, 0777)
				if err != nil {
					return err
				}
			}
		}

		path := filepath.Join(targetDir, header.Name)
		relativePath, err := filepath.Rel(targetDir, path)
		if err != nil {
			return err
		}

		if strings.HasPrefix(relativePath, ".."+string(os.PathSeparator)) {
			return fmt.Errorf("Archive attempting to write outside target dir")
		}

		info, err := os.Lstat(path)
		if err == nil {
			// File / dir already exists.
			if info.IsDir() && header.Typeflag != tar.TypeDir {
				// Existing dir, but want to write file.
				return fmt.Errorf("Cannot overwrite dir with file")
			}
			if !info.IsDir() && header.Typeflag == tar.TypeDir {
				// Existing file, but want to write dir.
				return fmt.Errorf("Cannot overwrite file with dir")
			}
			if info.IsDir() && header.Name == "." {
				continue
			}
			if !(info.IsDir() && header.Typeflag == tar.TypeDir) {
				err = os.RemoveAll(path)
				if err != nil {
					return err
				}
			}
		}

		readerBuffer.Reset(tarStream)
		err = unpackFile(path, targetDir, header, readerBuffer)
		if err != nil {
			return err
		}
	}
	return nil
}

func unpackFile(
	path string, targetDir string, header *tar.Header,
	reader *bufio.Reader) error {
	info := header.FileInfo()

	switch header.Typeflag {
	case tar.TypeDir:
		// Create dir if it does not exist. Do nothing if it already exists
		// (we want to merge).
		existingInfo, err := os.Lstat(path)
		if err != nil || !existingInfo.IsDir() {
			err = os.Mkdir(path, info.Mode())
			if err != nil {
				return err
			}
		}

	case tar.TypeReg, tar.TypeRegA: // Regular file.
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		_, err = io.Copy(file, reader)
		if err != nil {
			file.Close()
			return err
		}
		file.Close()

	case tar.TypeBlock, tar.TypeChar, tar.TypeFifo, tar.TypeXGlobalHeader:
		// Not supported. Skip.

	case tar.TypeLink: // Hard link.
		linkPath := filepath.Join(targetDir, header.Linkname)
		if !strings.HasPrefix(linkPath, targetDir) {
			return fmt.Errorf("Invalid hard link pointing outside target dir")
		}
		err := os.Link(linkPath, path)
		if err != nil {
			return err
		}

	case tar.TypeSymlink:
		linkPath := filepath.Join(filepath.Dir(path), header.Linkname)
		if !strings.HasPrefix(linkPath, targetDir) {
			return fmt.Errorf("Invalid sym link pointing outside target dir")
		}
		err := os.Symlink(linkPath, path)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("Unhandled tar header type")
	}

	for key, value := range header.Xattrs {
		err := syscall.Setxattr(path, key, []byte(value), 0)
		if err != nil {
			return err
		}
	}
	return nil
}
