#pragma once

#include "constant_mappings.hpp"

namespace opossum {

struct TableColumnDefinition final {
  TableColumnDefinition() = default;
  TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable = false);

  bool operator==(const TableColumnDefinition& rhs) const;

  std::string name;
  DataType data_type{DataType::Int};
  bool nullable{false};
};

// So that google test, e.g., prints readable error messages
inline std::ostream& operator<<(std::ostream& stream, const TableColumnDefinition& definition) {
  stream << definition.name << " ";
  stream << data_type_to_string.left.at(definition.data_type) << " ";
  stream << (definition.nullable ? "nullable" : "not nullable");
  return stream;
}

using TableColumnDefinitions = std::vector<TableColumnDefinition>;

TableColumnDefinitions concatenated(const TableColumnDefinitions& lhs, const TableColumnDefinitions& rhs);

}  // namespace opossum
