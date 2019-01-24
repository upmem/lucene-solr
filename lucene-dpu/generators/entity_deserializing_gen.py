import sys

if len(sys.argv) != 5:
    print "usage {} <entity file> <java package> <C output directory> <java output directory>".format(sys.argv[0])
    exit(1)

input_file_name = sys.argv[1]
java_package = sys.argv[2]
c_output_directory = sys.argv[3]
java_output_directory = sys.argv[4]

input_file = open(input_file_name, "r")

c_output_file = open("{}/{}.h".format(c_output_directory, input_file_name), "w")
java_output_file = open("{}/{}.java".format(java_output_directory, input_file_name), "w")

c_output_file.write("#ifndef _{}_H\n".format(input_file_name.upper()))
c_output_file.write("#define _{}_H\n".format(input_file_name.upper()))
c_output_file.write("\n")
c_output_file.write("#include <stdint.h>\n")
c_output_file.write("\n")
c_output_file.write("typedef struct {\n")

java_output_file.write("package {};\n".format(java_package))
java_output_file.write("\n")
java_output_file.write("import java.nio.ByteBuffer;\n")
java_output_file.write("\n")
java_output_file.write("public class {} {{\n".format(input_file_name.capitalize()))
java_output_file.write("\tprivate {}() {{}}\n".format(input_file_name.capitalize()))
java_output_file.write("\n")
java_output_file.write("\tpublic static {} deserialize(ByteBuffer buffer) {{\n".format(input_file_name.capitalize()))
java_output_file.write("\t\t{} entity = new {}();\n".format(input_file_name.capitalize(), input_file_name.capitalize()))
java_output_file.write("\n")

types = {
        "u8": ("uint8_t", "byte"),
        "i8": ("int8_t", "byte"),
        "u16": ("uint16_t", "short"),
        "i16": ("int16_t", "short"),
        "u32": ("uint32_t", "int"),
        "i32": ("int32_t", "int"),
        "u64": ("uint64_t", "long"),
        "i64": ("int64_t", "long"),
        }

java_fields = []

for line in input_file:
    words = line.split("=")
    key = words[0].strip()
    value = words[1].strip()
    c_type = types[value][0]
    java_type = types[value][1]
    java_fields.append((java_type, key))

    c_output_file.write("\t{} {};\n".format(c_type, key))
    java_output_file.write("\t\tthis.{} = buffer.get{}();\n".format(key, java_type.capitalize()))

c_output_file.write("}} {}_t;\n".format(input_file_name))
c_output_file.write("\n")
c_output_file.write("#endif\n")

java_output_file.write("\n")
java_output_file.write("\t\treturn entity;\n".format(input_file_name.capitalize()))
java_output_file.write("\t}\n")
java_output_file.write("\n")

for field in java_fields:
    java_output_file.write("\tpublic {} {};\n".format(field[0], field[1]))

java_output_file.write("}\n")

c_output_file.close()
java_output_file.close()
input_file.close()
