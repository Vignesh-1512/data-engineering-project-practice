from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    FloatType,
    BooleanType,
    ArrayType
)


def map_type(type_str: str):
    type_str = type_str.lower()

    mapping = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType()
    }

    if type_str not in mapping:
        raise ValueError(f"Unsupported data type: {type_str}")

    return mapping[type_str]


def build_schema(schema_dict: dict) -> StructType:
    """
    Recursively builds Spark StructType from YAML schema dictionary.
    """

    fields = []

    for key, value in schema_dict.items():

        # --------------------------
        # Primitive type
        # --------------------------
        if isinstance(value, str):
            fields.append(
                StructField(key, map_type(value), True)
            )

        # --------------------------
        # Array of Struct
        # --------------------------
        elif isinstance(value, dict) and "__type__" in value:

            if value["__type__"] == "array":

                struct_fields = build_schema(value["fields"])

                fields.append(
                    StructField(
                        key,
                        ArrayType(struct_fields),
                        True
                    )
                )

            else:
                raise ValueError(f"Unsupported complex type for {key}")

        # --------------------------
        # Nested Struct
        # --------------------------
        elif isinstance(value, dict):

            nested_struct = build_schema(value)

            fields.append(
                StructField(
                    key,
                    nested_struct,
                    True
                )
            )

        else:
            raise ValueError(f"Invalid schema definition for {key}")

    return StructType(fields)
