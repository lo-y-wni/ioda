/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "ioda/Engines/ODC/ParsedColumnExpression.h"
#include "oops/util/AssociativeContainers.h"  // for oops::contains

#if USE_BOOST_REGEX
#include <boost/regex.hpp>
#define REGEX_NAMESPACE boost
#else
#include <regex>
#define REGEX_NAMESPACE std
#endif

namespace ioda {
namespace Engines {
namespace ODC {

ParsedColumnExpression::ParsedColumnExpression(const std::string &expression) {
  static const REGEX_NAMESPACE::regex re("(\\w+)(?:\\.(\\w+))?(?:@(.+))?");
  REGEX_NAMESPACE::smatch match;
  if (REGEX_NAMESPACE::regex_match(expression, match, re)) {
    // This is an identifier of the form column[.member][@table]
    column += match[1].str();
    if (match[3].length()) {
       column += '@';
       column += match[3].str();
    }
    member = match[2];
  } else {
    // This is a more complex expression
    column = expression;
  }
}

bool isSourceInQuery(const ParsedColumnExpression &source,
                     const std::set<ParsedColumnExpression> &queryContents) {
  if (source.member.empty())
    return oops::contains(queryContents, source);
  else
    return oops::contains(queryContents, source) ||
           oops::contains(queryContents, ParsedColumnExpression(source.column));
}

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
