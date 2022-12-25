//
// Created by jinhai on 22-12-24.
//

#include <gtest/gtest.h>
#include "base_test.h"
#include "common/column_vector/column_vector.h"
#include "common/types/value.h"
#include "main/logger.h"
#include "main/stats/global_resource_usage.h"
#include "function/cast/uuid_cast.h"
#include "common/types/info/varchar_info.h"

class UuidCastTest : public BaseTest {
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

TEST_F(UuidCastTest, uuid_cast0) {
    using namespace infinity;

    // Try to cast uuid type to wrong type.
    {
        char uuid_str[17] = "aabbccddeeffgghh";
        UuidT source;
        source.Set(uuid_str);

        TinyIntT target;
        EXPECT_THROW(UuidTryCastToVarlen::Run(source, target, nullptr), FunctionException);
    }
    {
        char uuid_str[17] = "aabbccddeeffgghh";
        UuidT source;
        source.Set(uuid_str);
        VarcharT target;

        auto varchar_info = VarcharInfo::Make(65);
        DataType data_type(LogicalType::kVarchar, varchar_info);
        ColumnVector col_varchar(data_type, ColumnVectorType::kFlat);
        col_varchar.Initialize();

        EXPECT_TRUE(UuidTryCastToVarlen::Run(source, target, &col_varchar));

        target.Reset(false);
    }
}

TEST_F(UuidCastTest, uuid_cast1) {
    using namespace infinity;

    // Call BindUuidCast with wrong type of parameters
    {
        DataType target_type(LogicalType::kDecimal16);
        EXPECT_THROW(BindUuidCast(target_type), TypeException);
    }

    DataType source_type(LogicalType::kUuid);
    ColumnVector col_source(source_type, ColumnVectorType::kFlat);
    col_source.Initialize();
    for (i64 i = 0; i < DEFAULT_VECTOR_SIZE; ++ i) {
        String s('a' + i % 26, 16);
        UuidT uuid(s.c_str());

        Value v = Value::MakeUuid(uuid);
        col_source.AppendValue(v);
    }
    for (i64 i = 0; i < DEFAULT_VECTOR_SIZE; ++ i) {
        String s('a' + i % 26, 16);
        UuidT uuid(s.c_str());
        Value vx = col_source.GetValue(i);
        EXPECT_EQ(vx.type().type(), LogicalType::kUuid);
        EXPECT_EQ(vx.value_.uuid, uuid);
    }
    // cast uuid column vector to varchar column vector
    {
        DataType target_type(LogicalType::kVarchar);
        auto source2target_ptr = BindUuidCast(target_type);
        EXPECT_NE(source2target_ptr.function, nullptr);

        ColumnVector col_target(target_type, ColumnVectorType::kFlat);
        col_target.Initialize();

        CastParameters cast_parameters;
        EXPECT_TRUE(source2target_ptr.function(col_source, col_target, DEFAULT_VECTOR_SIZE, cast_parameters));

        for(i64 i = 0; i < DEFAULT_VECTOR_SIZE; ++ i) {
            String s('a' + i % 26, 16);
            UuidT uuid(s.c_str());
            String uuid_str(uuid.body, 16);

            Value vx = col_target.GetValue(i);
            EXPECT_EQ(vx.type().type(), LogicalType::kVarchar);
            EXPECT_FALSE(vx.is_null());
            EXPECT_STREQ(vx.value_.varchar.ToString().c_str(), uuid_str.c_str());
        }
    }
}