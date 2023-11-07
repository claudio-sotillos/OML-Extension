var duckdb = require('../../duckdb/tools/nodejs');
var assert = require('assert');

describe(`oml_reader extension`, () => {
    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"});
        conn = new duckdb.Connection(db);
        conn.exec(`LOAD '${process.env.QUACK_EXTENSION_BINARY_PATH}';`, function (err) {
            if (err) throw err;
            done();
        });
    });

    it('oml_reader function should return expected string', function (done) {
        db.all("SELECT oml_reader('Sam') as value;", function (err, res) {
            if (err) throw err;
            assert.deepEqual(res, [{value: "OmlReader Sam 🐥"}]);
            done();
        });
    });

    it('oml_reader_openssl_version function should return expected string', function (done) {
        db.all("SELECT oml_reader_openssl_version('Michael') as value;", function (err, res) {
            if (err) throw err;
            assert(res[0].value.startsWith('OmlReader Michael, my linked OpenSSL version is OpenSSL'));
            done();
        });
    });
});