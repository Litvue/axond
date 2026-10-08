import assert from "node:assert/strict";
import test from "node:test";

import { isJsonContentType } from "./content-type.ts";

test("json_content_type_parser_matches_mime", () => {
  const cases: Array<[string, boolean]> = [
    ["application/json", true],
    ["application/json; charset=utf-8", true],
    ["application/json;charset=utf-8", true],
    ["application/cloudevents+json", true],
    ["text/json", false],
    ["application/jsonfoo", false],
    ["fooapplication/json", false],
    ["APPLICATION/JSON", true],
    ["Application/Json; Charset=UTF-8", true],
    ["application/vnd.api+json", true],
    ["application/problem+json", true],
    ["application/json+problem", true],
    ["application/foo+bar+json", true],
    ["application/foo+json+xml", false],
    ["application/json+", true],
    ["application/+json", false],
    ["application/+foo+json", true],
    ["application/json;", true],
    ["application/json; ", true],
    ["application/json;  ", true],
    ["application/json; charset", false],
    [" application/json", false],
    ["application/json ", false],
    ["application/json, text/plain", false],
    ['application/json; charset="utf-8"', true],
    ['application/json;charset="UTF-8"', true],
    ["", false],
    ["application/", false],
    ["application/json; boundary=x", true],
    ["application/json; charset=utf-8; foo=bar", true],
    ["application/json;foo=bar", true],
    ["text/plain", false],
    ["application/xml", false],
    ["application/json+json", true],
    ["application/JSON", true],
    ["application/x-json", false],
    ["application/json; charset=utf-8 ", false],
    ["application/json\t", false],
    ["application/json\n", false],
    ["*/*", false],
    ["application/*", false],
    ["application/*+json", true],
    ["application/json; charset=latin1", true],
  ];
  for (const [header, accepted] of cases) {
    assert.equal(isJsonContentType(header), accepted, JSON.stringify(header));
  }
});
