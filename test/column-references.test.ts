import test from "ava"
import * as path from "path"
import { loadSourceFile, parseSourceFile } from "../src/parse-file"
import { validateQuery } from "../src/validation"
import { containsToRegex } from "./_helpers/assert"

const pathToFixture = (fileName: string) => path.join(__dirname, "_fixtures", fileName)

test("fails on bad unqualified column reference", t => {
  const { queries, tableSchemas } = parseSourceFile(
    loadSourceFile(pathToFixture("column-reference-unqualified.ts"))
  )
  const error = t.throws(() => queries.forEach(query => validateQuery(query, tableSchemas)))
  t.is(error.name, "ValidationError")
  t.regex(error.message, containsToRegex(`No table in the query's scope has a column "password".`))
  t.regex(error.message, containsToRegex(`_fixtures/column-reference-unqualified.ts:11:15`))
})

test("fails on bad qualified column reference", t => {
  const { queries, tableSchemas } = parseSourceFile(
    loadSourceFile(pathToFixture("column-reference-qualified.ts"))
  )
  const error = t.throws(() => queries.forEach(query => validateQuery(query, tableSchemas)))
  t.is(error.name, "ValidationError")
  t.regex(error.message, containsToRegex(`Table "projects" does not have a column named "email".`))
  t.regex(error.message, containsToRegex(`_fixtures/column-reference-qualified.ts:16:23`))
})

test("fails on bad column reference in INSERT", t => {
  const { queries, tableSchemas } = parseSourceFile(
    loadSourceFile(pathToFixture("column-reference-insert.ts"))
  )
  const error = t.throws(() => queries.forEach(query => validateQuery(query, tableSchemas)))
  t.is(error.name, "ValidationError")
  t.regex(error.message, containsToRegex(`No table in the query's scope has a column "foo".`))
  t.regex(error.message, containsToRegex(`_fixtures/column-reference-insert.ts:13:31`))
})
