#pragma once

#include "storage/table.hpp"

class sqlite3_stmt;
class sqlite3;

namespace opossum {

/*
 * This class wraps the sqlite3 library for opossum. It creates an in-memory sqlite database on construction.
 * When executing a sql query, the wrapper converts the result into an opossum Table.
 */
class SQLiteWrapper final {
 public:
  SQLiteWrapper();
  ~SQLiteWrapper();

  /*
   * Creates a table in the sqlite database from a given .tbl file.
   *
   * @param file Path to .tbl file
   * @param tablename The desired table name
   */
  void create_table_from_tbl(const std::string& file, const std::string& table_name);

  /*
   * Recreates a table given another table to copy from.
   *
   * @param command SQL command string
   */
  void reset_table_from_copy(const std::string& table_name_to_reset, const std::string& table_name_to_copy_from) const;

  /*
   * Creates a table in the sqlite database from a given opossum Table
   *
   * @param table      The table to load into sqlite
   * @param tablename  The desired table name
   */
  void create_table(const Table& table, const std::string& table_name);

  /*
   * Executes a sql query in the sqlite database context.
   *
   * @param sql_query Query to be executed
   * @returns An opossum Table containing the results of the executed query
   */
  std::shared_ptr<Table> execute_query(const std::string& sql_query);

 protected:
  /*
   * Creates columns in given opossum table according to an sqlite intermediate statement (one result row).
   */
  std::shared_ptr<Table> _create_table(sqlite3_stmt* result_row, int column_count);

  /*
   * Adds a single row to given opossum table according to an sqlite intermediate statement (one result row).
   */
  void _add_row(const std::shared_ptr<Table>& table, sqlite3_stmt* result_row, int column_count);

  /**
   * Execute an SQL statement on the wrapped sqlite db
   */
  void _exec_sql(const std::string& sql) const;

  sqlite3* _db;
};

}  // namespace opossum
