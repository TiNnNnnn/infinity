// Copyright(C) 2023 InfiniFlow, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module;

#include <sstream>

module logical_top;

import stl;
import logical_node_type;
import column_binding;
import logical_node;
import parser;
import base_expression;

namespace infinity {

Vector<ColumnBinding> LogicalTop::GetColumnBindings() const { return LogicalCommonFunctionUsingLoadMeta::GetColumnBindings(*this); }

SharedPtr<Vector<String>> LogicalTop::GetOutputNames() const { return LogicalCommonFunctionUsingLoadMeta::GetOutputNames(*this); }

SharedPtr<Vector<SharedPtr<DataType>>> LogicalTop::GetOutputTypes() const { return LogicalCommonFunctionUsingLoadMeta::GetOutputTypes(*this); }

String LogicalTop::ToString(i64 &space) const {
    std::stringstream ss;
    String arrow_str;
    if (space > 3) {
        space -= 4;
        arrow_str = "->  ";
    }

    ss << String(space, ' ') << arrow_str << "Top: Limit (limit: " << limit_expression_->Name();
    if (offset_expression_) {
        ss << ", offset: " << offset_expression_->Name();
    }
    ss << ") Order by: ";
    SizeT expression_count = sort_expressions_.size();
    for (SizeT i = 0; i < expression_count - 1; ++i) {
        ss << sort_expressions_[i]->Name() << " " << SelectStatement::ToString(order_by_types_[i]) << ", ";
    }
    ss << sort_expressions_.back()->Name() << " " << SelectStatement::ToString(order_by_types_.back());
    space += arrow_str.size();

    return std::move(ss).str();
}

} // namespace infinity
