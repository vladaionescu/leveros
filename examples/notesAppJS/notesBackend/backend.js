
'use strict';

const MongoClient = require('mongodb').MongoClient;
const lodash = require('lodash');

const mongoUrl = 'mongodb://172.17.0.1:27017/notesApp'

function addNote(note, callback) {
    getDb((err, db) => {
        if (err) {
            callback(err);
            return;
        }

        db.collection('notes').insertOne({
            note,
        }, (err, result) => {
            if (err) {
                console.log(err);
                callback(err);
                return;
            }

            callback(null, null);
        });
    });
}
module.exports.addNote = addNote;

function getNotes(callback) {
    getDb((err, db) => {
        if (err) {
            callback(err);
            return;
        }

        db.collection('notes').find({}).toArray((err, docs) => {
            if (err) {
                console.log(err);
                callback(err);
                return;
            }

            const notes = lodash.map(docs, (doc) => {
                return doc.note;
            });
            callback(null, notes);
        });
    });
}
module.exports.getNotes = getNotes;

let cachedDb = null;
function getDb(callback) {
    if (cachedDb !== null) {
        callback(null, cachedDb);
        return;
    }
    MongoClient.connect(mongoUrl, (err, db) => {
        if (err) {
            console.log(err);
            callback(err);
            return;
        }
        cachedDb = db;
        callback(null, db);
    });
}
