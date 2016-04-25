module.exports = function (grunt) {
    grunt.loadNpmTasks('grunt-babel');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-eslint');

    var config = {
        compiled_dir: 'compiled',
    };

    var task = {
        clean: [
            '<%= compiled_dir %>',
        ],
        babel: {
            options: {
                sourceMaps: 'inline',
                presets: ['es2015'],
            },
            leveros: {
                expand: true,
                cwd: './',
                src: 'lib/**/*.js',
                dest: '<%= compiled_dir %>/',
            },
        },
        eslint: {
            leveros: {
                src: 'lib/**/*.js',
            },
        },
    };

    grunt.initConfig(grunt.util._.extend(task, config));

    grunt.registerTask('compile', ['babel:leveros']);
    grunt.registerTask('lint', ['eslint:leveros']);
    grunt.registerTask('default', ['compile']);
}
