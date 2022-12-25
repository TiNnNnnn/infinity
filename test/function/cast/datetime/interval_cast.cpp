//
// Created by jinhai on 22-12-23.
//


#include <gtest/gtest.h>
#include "base_test.h"
#include "common/column_vector/column_vector.h"
#include "common/types/value.h"
#include "main/logger.h"
#include "main/stats/global_resource_usage.h"
#include "function/cast/interval_cast.h"
#include "common/types/info/varchar_info.h"

class IntervalCastTest : public BaseTest {
    void
    SetUp() override {
        infinity::Logger::Initialize();
        infinity::GlobalResourceUsage::Init();
    }

    void
    TearDown() override {
        infinity::Logger::Shutdown();
        EXPECT_EQ(infinity::GlobalResourceUsage::GetObjectCount(), 0);
        EXPECT_EQ(infinity::GlobalResourceUsage::GetRawMemoryCount(), 0);
        infinity::GlobalResourceUsage::UnInit();
    }
};

TEST_F(IntervalCastTest, date_cast0) {
    using namespace infinity;

    // Try to cast Interval type to wrong type.
    {
        IntervalT source;
        TinyIntT target;
        EXPECT_THROW(IntervalTryCastToVarlen::Run(source, target, nullptr), FunctionException);
    }
    {
        IntervalT source;
        VarcharT target;

        auto varchar_info = VarcharInfo::Make(65);
        DataType data_type(LogicalType::kVarchar, varchar_info);
        ColumnVector col_varchar(data_type, ColumnVectorType::kFlat);
        col_varchar.Initialize();

        EXPECT_THROW(IntervalTryCastToVarlen::Run(source, target, &col_varchar), NotImplementException);
    }
}


TEST_F(IntervalCastTest, date_cast1) {
    using namespace infinity;

    // Call BindIntervalCast with wrong type of parameters
    {
        DataType target_type(LogicalType::kDecimal16);
        EXPECT_THROW(BindTimeCast(target_type), TypeException);
    }

    DataType source_type(LogicalType::kInterval);
    ColumnVector col_source(source_type, ColumnVectorType::kFlat);
    col_source.Initialize();
    for (i64 i = 0; i < DEFAULT_VECTOR_SIZE; ++ i) {
        Value v = Value::MakeInterval(IntervalT(static_cast<i32>(i)));
        col_source.AppendValue(v);
        Value vx = col_source.GetValue(i);
    }
    for (i64 i = 0; i < DEFAULT_VECTOR_SIZE; ++ i) {
        Value vx = col_source.GetValue(i);
        EXPECT_EQ(vx.type().type(), LogicalType::kInterval);
        EXPECT_FLOAT_EQ(vx.value_.interval.value, static_cast<i32>(i));
    }
    // cast interval column vector to varchar column vector
    {
        DataType target_type(LogicalType::kVarchar);
        auto source2target_ptr = BindTimeCast(target_type);
        EXPECT_NE(source2target_ptr.function, nullptr);

        ColumnVector col_target(target_type, ColumnVectorType::kFlat);
        col_target.Initialize();

        CastParameters cast_parameters;
        EXPECT_THROW(source2target_ptr.function(col_source, col_target, DEFAULT_VECTOR_SIZE, cast_parameters), NotImplementException);
    }
}