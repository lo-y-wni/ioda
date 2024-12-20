#pragma once
/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <set>
#include <string>

namespace ioda {
namespace Engines {
namespace ODC {

/// Parsed SQL column expression.
struct ParsedColumnExpression {
  /// If `expression` is a bitfield column member name (of the form `column.member[@table]`,
  /// where `@table` is optional), split it into the column name `column[@table]` and member
  /// name `member`. Otherwise leave it unchanged.
  explicit ParsedColumnExpression(const std::string &expression);

  std::string column;  //< Column name (possibly including table name) or a more general expression
  std::string member;  //< Bitfield member name (may be empty)
};

inline bool operator<(const ParsedColumnExpression & a, const ParsedColumnExpression &b) {
  return a.column < b.column || (a.column == b.column && a.member < b.member);
}

bool isSourceInQuery(const ParsedColumnExpression &source,
                     const std::set<ParsedColumnExpression> &queryContents);

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
