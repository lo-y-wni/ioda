#pragma once
/*
 * (C) Copyright 2021 Met Office UK
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */
/*! \defgroup ioda_cxx_layout_internal Internal Groups and Data Layout
 * \brief Private API
 * \ingroup ioda_internals
 *
 * @{
 * \file Layout_ObsGroup_ODB_Params.h
 * \brief Defines all of the information which should be stored in the YAML mapping file.
 */

#include <string>
#include <vector>

#include "ioda/config.h"  // Auto-generated. Defines *_FOUND.
#if odc_FOUND
# include "ioda/Engines/ODC/VariableReaderFactory.h"
#endif
#include "oops/util/parameters/OptionalParameter.h"
#include "oops/util/parameters/Parameter.h"
#include "oops/util/parameters/Parameters.h"
#include "oops/util/parameters/RequiredParameter.h"
#include "oops/util/parameters/RequiredPolymorphicParameter.h"

namespace ioda {
namespace detail {

enum class IoMode {
  READ, WRITE, READ_AND_WRITE
};

struct IoModeParameterTraitsHelper {
  using EnumType = IoMode;
  static constexpr char enumTypeName[] = "IoMode";
  static constexpr util::NamedEnumerator<IoMode> namedValues[] = {
    { IoMode::READ, "read" },
    { IoMode::WRITE, "write" },
    { IoMode::READ_AND_WRITE, "read and write" },
  };
};

constexpr char IoModeParameterTraitsHelper::enumTypeName[];
constexpr util::NamedEnumerator<IoMode> IoModeParameterTraitsHelper::namedValues[];

}  // namespace detail
}  // namespace ioda

namespace oops {

template <>
struct ParameterTraits<ioda::detail::IoMode> :
  public EnumParameterTraits<ioda::detail::IoModeParameterTraitsHelper>
{};

} // namespace oops

namespace ioda {
namespace detail {

/// \brief A container for the configuration options of an object extracting variable values from
/// a varno-independent column.
class VariableReaderParameters : public oops::Parameters {
  OOPS_CONCRETE_PARAMETERS(VariableReaderParameters, Parameters)
 public:
#if odc_FOUND
  /// After deserialization, holds an instance of a VariableReaderParametersBase subclass
  /// controlling the reader's behavior. The subclass type is determined by the value of the
  /// "type" key in the Configuration object from which this object is deserialized.
  oops::RequiredPolymorphicParameter<Engines::ODC::VariableReaderParametersBase,
                                     Engines::ODC::VariableReaderFactory> params{"type", this};
#endif
};

/// Defines the mapping between an ioda variable and an ODB column storing values dependent
/// on the observation location, but not on the observed variable (varno), like most metadata.
class VariableParameters : public oops::Parameters {
OOPS_CONCRETE_PARAMETERS(VariableParameters, Parameters)
 public:
  /// \p name is what the variable should be referred to as in ioda, including the full group
  /// hierarchy.
  oops::RequiredParameter<std::string> name {"name", this};
  /// \p source is the name of the column storing the variable values in an ODB file.
  oops::RequiredParameter<std::string> source {"source", this};
  /// \p unit is the variable's unit type, for conversion to SI units. The data values will be
  /// changed according to the arithmetic conversion function if function is available.
  oops::OptionalParameter<std::string> unit {"unit", this};
  /// \p bitIndex can be used to specify the index of a bit within a bitfield that should
  /// store the value of a Boolean variable when writing an ODB file. Currently not used;
  /// will be used by the ODB writer.
  oops::OptionalParameter<int> bitIndex {"bit index", this};
  /// \p multichannel should be set to true for variables with a Channel dimension.
  oops::Parameter<bool> multichannel{"multichannel", false, this};
  /// \p reader can be set to a dictionary specifying the type and configuration of a custom
  /// variable reader, i.e. an object extracting data from the ODB column `source` into an ioda
  /// variable. Defaults to the reader specified by the `default reader` option in the
  /// OdbQueryParameters.
  oops::OptionalParameter<VariableReaderParameters> reader{"reader", this};
  /// \p mode can be set to IoMode::READ if the current mapping should be used only by the ODB
  /// reader but not by the ODB writer, or to IoMode::WRITE if it should be used only by the writer.
  /// By default, mappings are used both by the reader and the writer.
  oops::Parameter<IoMode> mode{"mode", IoMode::READ_AND_WRITE, this};
};

class ComplementaryVariablesParameters : public oops::Parameters {
OOPS_CONCRETE_PARAMETERS(ComplementaryVariablesParameters, Parameters)
 public:
  /// \p outputName is the variable's name as it should be found in IODA. The full group
  /// hierarchy should be included.
  oops::RequiredParameter<std::string> outputName {"output name", this};
  /// \p outputVariableDataType is the output variable's data type. Strings are currently
  /// the only supported type.
  oops::Parameter<std::string> outputVariableDataType {
    "output variable data type", "string", this};
  /// \p inputNames are the variable names as they should be found prior to the merge.
  oops::RequiredParameter<std::vector<std::string>> inputNames {"input names", this};
  /// \p mergeMethod is the method which should be used to combine the input variables.
  oops::Parameter<std::string> mergeMethod {"merge method", "concat", this};
};

/// Maps a varno to an ioda variable name (without group).
class VarnoToVariableNameMappingParameters : public oops::Parameters {
OOPS_CONCRETE_PARAMETERS(VarnoToVariableNameMappingParameters, Parameters)
 public:
  /// ioda variable name. Example: \c brightnessTemperature.
  oops::RequiredParameter<std::string> name {"name", this};
  /// ODB identifier of an observed variable. Example: \c 119.
  oops::RequiredParameter<int> varno {"varno", this};
  /// ODB identifiers of any other observed variables to be merged in the same ioda variable.
  oops::Parameter<std::vector<int>> auxiliaryVarnos {"auxiliary varnos", {}, this};
  /// (Optional) The non-SI unit in which the variable values are expressed in the ODB
  /// file. These values will be converted to SI units before storing in the ioda variable.
  oops::OptionalParameter<std::string> unit {"unit", this};
};

/// Defines the mapping between a set of ioda variables and an ODB column storing values dependent
/// not just on the observation location, like most metadata, but also on the observed
/// variable (varno), like obs values, obs errors, QC flags and diagnostic flags.
class VarnoDependentColumnParameters : public oops::Parameters {
OOPS_CONCRETE_PARAMETERS(VarnoDependentColumnParameters, Parameters)
 public:
  /// ODB column name. Example: \c initial_obsvalue.
  oops::RequiredParameter<std::string> source {"source", this};
  /// Name of the ioda group containing the variables storing restrictions
  /// of the ODB column `source` to individual varnos. Example: \c ObsValue.
  oops::RequiredParameter<std::string> groupName {"group name", this};
  /// Specified the index of a bit within a bitfield that should
  /// store the value of a Boolean variable when writing an ODB file. Currently not used;
  /// will be used by the ODB writer.
  oops::OptionalParameter<int> bitIndex {"bit index", this};
  /// Maps varnos to names of variables storing restrictions of the ODB column `source` to
  /// these varnos.
  oops::Parameter<std::vector<VarnoToVariableNameMappingParameters>> mappings {
    "varno-to-variable-name mapping", {}, this};
};

class ODBLayoutParameters : public oops::Parameters {
OOPS_CONCRETE_PARAMETERS(ODBLayoutParameters, Parameters)
 public:
  oops::Parameter<std::vector<VariableParameters>> variables {"varno-independent columns", {}, this};
  oops::Parameter<std::vector<ComplementaryVariablesParameters>> complementaryVariables {
    "complementary variables", {}, this};
  oops::Parameter<std::vector<VarnoDependentColumnParameters>> varnoDependentColumns {
    "varno-dependent columns", {}, this};
};

}  // namespace detail
}  // namespace ioda
