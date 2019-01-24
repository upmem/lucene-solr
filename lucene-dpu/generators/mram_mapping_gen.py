import sys

if len(sys.argv) != 5:
    print "usage {} <mapping file> <java package> <C output directory> <java output directory>".format(sys.argv[0])
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

java_output_file.write("package {};\n".format(java_package))
java_output_file.write("\n")
java_output_file.write("public class {} {{\n".format(input_file_name.capitalize()))
java_output_file.write("\tprivate {}() {{}}\n".format(input_file_name.capitalize()))
java_output_file.write("\n")

for line in input_file:
    words = line.split("=")
    key = words[0].strip()
    value = words[1].strip()
    c_output_file.write("#define {} ({})\n".format(key, value))
    java_output_file.write("\tpublic static final int {} = {};\n".format(key, value))

c_output_file.write("\n")
c_output_file.write("#endif\n")

java_output_file.write("}\n")

c_output_file.close()
java_output_file.close()
input_file.close()
