CREATE TABLE "priam_test" (
	"column1" ascii,
	"column2" ascii,
	"column3" map<text, text>,
	PRIMARY KEY ("column1", "column2")
);

INSERT INTO "priam_test" ("column1", "column2", "column3") VALUES ('hello', 'world', { 'key1': 'val1', 'key2': 'val2' });