import { NodePath } from "@babel/traverse"
import * as types from "@babel/types"
import * as QueryParser from "pg-query-parser"
import { getReferencedNamedImport } from "./babel-parser-utils"
import { augmentBabelSyntaxError, augmentFileValidationError, augmentQuerySyntaxError } from "./errors"
import {
  createQueryNodePath,
  findParentQueryStatement,
  getNodeType,
  traverseQuery,
  traverseSubTree,
  QueryNodePath
} from "./query-parser-utils"

export interface TableReference {
  tableName: string,
  as?: string,
  path: QueryNodePath<any>
}

export interface QualifiedColumnReference {
  tableName: string,
  columnName: string,
  path: QueryNodePath<any>
}

export interface UnqualifiedColumnReference {
  tableRefsInScope: TableReference[],
  columnName: string,
  path: QueryNodePath<any>
}

export type ColumnReference = QualifiedColumnReference | UnqualifiedColumnReference

export interface Query {
  filePath: string,
  query: string,
  referencedColumns: ColumnReference[],
  referencedTables: TableReference[],
  loc: types.SourceLocation | null
}

const isColumnRef = (node: QueryParser.QueryNode<any>): node is QueryParser.ColumnRef => "ColumnRef" in node
const isPgString = (node: QueryParser.QueryNode<any>): node is QueryParser.PgString => "String" in node
const isRelationRef = (node: QueryParser.QueryNode<any>): node is QueryParser.RelationRef => "RangeVar" in node

function filterDuplicateTableRefs (tableRefs: TableReference[]) {
  return tableRefs.reduce(
    (filtered, ref) => filtered.find(someRef => JSON.stringify(someRef) === JSON.stringify(ref)) ? filtered : [...filtered, ref],
    [] as TableReference[]
  )
}

function isSpreadInsertExpression (expression: NodePath<any>): expression is NodePath<types.CallExpression> {
  if (!expression.isCallExpression()) return false

  const callee = expression.get("callee")
  if (!callee.isIdentifier()) return false

  const importSpecifier = getReferencedNamedImport(callee)
  return Boolean(importSpecifier && importSpecifier.node.name === "spreadInsert")
}

function parsePostgresQuery (query: string, path: NodePath<types.TemplateLiteral>, filePath: string) {
  const result = QueryParser.parse(query)

  if (result.error) {
    const error = new Error(`Syntax error in SQL query.`)
    throw augmentFileValidationError(augmentQuerySyntaxError(error, result.error), {
      filePath,
      query,
      referencedColumns: [],
      referencedTables: [],
      loc: path.node.loc
    })
  }

  return result.query[0]
}

function getTableReferences (statementPath: QueryNodePath<any>, includeSubQueries: boolean): QueryNodePath<QueryParser.RelationRef>[] {
  const referencedTables: QueryNodePath<QueryParser.RelationRef>[] = []

  traverseSubTree(statementPath.node, statementPath.ancestors, (path) => {
    const { node } = path
    if (isRelationRef(node)) {
      referencedTables.push(path)
    } else if (!includeSubQueries) {
      if (node !== statementPath.node && path.type.endsWith("Stmt")) return false
    }
  })

  return referencedTables
}

function resolveTableName (tableIdentifier: string, tableRefs: QueryNodePath<QueryParser.RelationRef>[]): string {
  const matchingTableRef = tableRefs.find(tableRef => {
    const refNode = tableRef.node
    return refNode.RangeVar.alias
      ? refNode.RangeVar.alias.Alias.aliasname === tableIdentifier
      : refNode.RangeVar.relname === tableIdentifier
  })

  if (matchingTableRef) {
    return matchingTableRef.node.RangeVar.relname
  } else {
    throw new Error(`No matching table reference found for "${tableIdentifier}".`)
  }
}

function getReferencedColumns (parsedQuery: QueryParser.Query): ColumnReference[] {
  const referencedColumns: ColumnReference[] = []

  traverseQuery(parsedQuery, path => {
    if (isColumnRef(path.node)) {
      const { fields } = path.node.ColumnRef
      const statement = findParentQueryStatement(path) || createQueryNodePath(parsedQuery, [])
      const relationRefs = getTableReferences(statement, false)

      if (fields.length === 1) {
        const [columnNode] = fields
        if (isPgString(columnNode)) {
          // Ignore `*` column references, since there is nothing to validate
          const tableRefsInScope: TableReference[] = relationRefs.map(ref => ({
            as: ref.node.RangeVar.alias ? ref.node.RangeVar.alias.Alias.aliasname : undefined,
            tableName: ref.node.RangeVar.relname,
            path
          }))
          referencedColumns.push({
            tableRefsInScope: filterDuplicateTableRefs(tableRefsInScope),
            columnName: columnNode.String.str,
            path
          })
        }
      } else if (fields.length === 2) {
        const [tableNode, columnNode] = fields
        if (!isPgString(tableNode)) {
          throw new Error(`Expected first identifier in column reference to be a string. Got ${getNodeType(tableNode)}`)
        }
        if (isPgString(columnNode)) {
          // Ignore `table.*` column references, since there is nothing to validate
          referencedColumns.push({
            tableName: resolveTableName(tableNode.String.str, relationRefs),
            columnName: columnNode.String.str,
            path
          })
        }
      } else {
        throw new Error(`Expected column reference to be of format <table>.<column> or <column>. Got: ${fields.join(".")}`)
      }
    }
  })
  return referencedColumns
}

function getSpreadInsertReferencedColumnNames (path: NodePath<types.CallExpression>): string[] {
  const args = path.get("arguments")
  const arg = args[0]

  if (args.length !== 1) {
    throw augmentBabelSyntaxError(new Error(`Expected spreadInsert() to be called with one argument. Is called with ${args.length}.`), path)
  }

  if (arg.isObjectExpression()) {
    const objectProperties = arg.get("properties")
      .filter(property => property.isObjectProperty()) as NodePath<types.ObjectProperty>[]

    return objectProperties.map(property => {
      const key = property.get("key")

      if (!Array.isArray(key) && key.isIdentifier()) {
        return key.node.name
      } else if (!Array.isArray(key) && key.isStringLiteral()) {
        return key.node.value
      }
      return null
    }).filter(columnName => Boolean(columnName)) as string[]
  } else {
    // Not statically analyzable without resolved TypeScript types
    return []
  }
}

export function parseQuery (path: NodePath<types.TemplateLiteral>, filePath: string): Query {
  const expressions = path.get("expressions")
  const textPartials = path.get("quasis").map(quasi => quasi.node.value.cooked)

  let templatedQueryString = textPartials[0]

  for (let index = 0; index < expressions.length; index++) {
    const expression = expressions[index]

    if (isSpreadInsertExpression(expression)) {
      templatedQueryString += `SELECT \$${index + 1}`
      // TODO: - Keep array of insert expressions incl. position in SQL query string
      //       - Iterate over them after for-loop
      //       - Reuse logic from getReferencedColumns() to get referenceable tables at each expression position
      //       - Add ColumnReference[]
    } else {
      templatedQueryString += `\$${index + 1}`
    }

    if (textPartials[index + 1]) {
      templatedQueryString += textPartials[index + 1]
    }
  }

  const parsedQuery = parsePostgresQuery(templatedQueryString, path, filePath)
  const referencedColumns = getReferencedColumns(parsedQuery)

  const referencedTables = getTableReferences(createQueryNodePath(parsedQuery, []), true).map(path => ({
    tableName: path.node.RangeVar.relname,
    path
  }))

  return {
    filePath,
    referencedColumns,
    referencedTables,
    query: templatedQueryString,
    loc: path.node.loc
  }
}
