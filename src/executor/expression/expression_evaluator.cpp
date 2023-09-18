//
// Created by jinhai on 23-2-7.
//

#include "expression_evaluator.h"
#include "expression/in_expression.h"

#include <utility>

namespace infinity {

void
ExpressionEvaluator::Init(const DataBlock* input_data_block) {
    input_data_block_ = input_data_block;
}

void
ExpressionEvaluator::Execute(const SharedPtr<BaseExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column) {

    switch(expr->type()) {
        case ExpressionType::kAggregate:
            return Execute(std::static_pointer_cast<AggregateExpression>(expr), state, output_column);
        case ExpressionType::kCast:
            return Execute(std::static_pointer_cast<CastExpression>(expr), state, output_column);
        case ExpressionType::kCase:
            return Execute(std::static_pointer_cast<CaseExpression>(expr), state, output_column);
        case ExpressionType::kColumn:
            return Execute(std::static_pointer_cast<ColumnExpression>(expr), state, output_column);
        case ExpressionType::kFunction:
            return Execute(std::static_pointer_cast<FunctionExpression>(expr), state, output_column);
        case ExpressionType::kValue:
            return Execute(std::static_pointer_cast<ValueExpression>(expr), state, output_column);
        case ExpressionType::kReference:
            return Execute(std::static_pointer_cast<ReferenceExpression>(expr), state, output_column);
        case ExpressionType::kIn:
            return Execute(std::static_pointer_cast<InExpression>(expr), state, output_column);
        default:
            ExecutorError("Unknown expression type: " + expr->Name());
    }
}

void
ExpressionEvaluator::Execute(const SharedPtr<AggregateExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column_vector) {
    ExecutorAssert(!in_aggregate_, "Recursive execute aggregate function!")
    in_aggregate_ = true;
    SharedPtr<ExpressionState>& child_state = state->Children()[0];
    SharedPtr<BaseExpression>& child_expr = expr->arguments()[0];
    // Create output chunk.
    // TODO: Now output chunk is pre-allocate memory in expression state
    // TODO: In the future, it can be implemented as on-demand allocation.
    SharedPtr<ColumnVector>& child_output = child_state->OutputColumnVector();
    Execute(child_expr, child_state, child_output);

    ExecutorAssert(expr->aggregate_function_.argument_type_ == *child_output->data_type(),
                   "Argument type isn't matched with the child expression output");
    ExecutorAssert(expr->aggregate_function_.return_type_ == *output_column_vector->data_type(),
                   "Return type isn't matched");

    // 1. Initialize the aggregate state.
    expr->aggregate_function_.init_func_(expr->aggregate_function_.GetState());

    // 2. Loop to fill the aggregate state
    expr->aggregate_function_.update_func_(expr->aggregate_function_.GetState(), child_output);

    // 3. Get the aggregate result and append to output column vector.

    ptr_t result_ptr = expr->aggregate_function_.finalize_func_(expr->aggregate_function_.GetState());
    output_column_vector->AppendByPtr(result_ptr);

    in_aggregate_ = false;
}

void
ExpressionEvaluator::Execute(const SharedPtr<CastExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column_vector) {
    SharedPtr<ExpressionState>& child_state = state->Children()[0];
    SharedPtr<BaseExpression>& child_expr = expr->arguments()[0];
    // Create output chunk.
    // TODO: Now output chunk is pre-allocate memory in expression state
    // TODO: In the future, it can be implemented as on-demand allocation.
    SharedPtr<ColumnVector>& child_output = child_state->OutputColumnVector();
    Execute(child_expr, child_state, child_output);

    CastParameters cast_parameters;

    expr->func_.function(child_output,
                         output_column_vector,
                         child_output->Size(),
                         cast_parameters);
}

void
ExpressionEvaluator::Execute(const SharedPtr<CaseExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column_vector) {
    ExecutorError("Case execution isn't implemented yet.");
}

void
ExpressionEvaluator::Execute(const SharedPtr<ColumnExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column_vector) {

    ExecutorError("ColumnExpression isn't expected here.");
}

void
ExpressionEvaluator::Execute(const SharedPtr<FunctionExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column_vector) {

    SizeT argument_count = expr->arguments().size();
    Vector<SharedPtr<ColumnVector>> arguments;
    arguments.reserve(argument_count);

    for(SizeT i = 0; i < argument_count; ++ i) {
        SharedPtr<ExpressionState>& argument_state = state->Children()[i];
        SharedPtr<BaseExpression>& argument_expr = expr->arguments()[i];
        SharedPtr<ColumnVector>& argument_output = argument_state->OutputColumnVector();
        Execute(argument_expr, argument_state, argument_output);
        arguments.emplace_back(argument_output);
    }

    DataBlock func_input_data_block;
    func_input_data_block.Init(arguments);

    expr->func_.function_(func_input_data_block, output_column_vector);
}

void
ExpressionEvaluator::Execute(const SharedPtr<ValueExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column_vector) {
    output_column_vector->SetValue(0, expr->GetValue());
    output_column_vector->Finalize(1);
}

void
ExpressionEvaluator::Execute(const SharedPtr<ReferenceExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column_vector) {
    SizeT column_index = expr->column_index();

    if(input_data_block_ == nullptr) {
        ExecutorError("Input data block is NULL")
    }
    if(column_index >= input_data_block_->column_count()) {
        ExecutorError("Invalid column index")
    }

    output_column_vector = input_data_block_->column_vectors[column_index];
}

void
ExpressionEvaluator::Execute(const SharedPtr<InExpression>& expr,
                             SharedPtr<ExpressionState>& state,
                             SharedPtr<ColumnVector>& output_column_vector) {
    ExecutorError("IN execution isn't implemented yet.");
}

}