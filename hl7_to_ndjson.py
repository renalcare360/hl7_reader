import os
import glob
import json
from hl7apy.parser import parse_message


def hl7_str_to_dict(s, use_long_name=True):
    s = s.replace("\n", "\r")
    m = parse_message(s)
    return hl7_message_to_dict(m, use_long_name=use_long_name)


def hl7_message_to_dict(m, use_long_name=True):
    if m.children:
        d = {}
        for c in m.children:
            name = str(c.name).lower()
            if use_long_name:
                name = str(c.long_name).lower() if c.long_name else name
            dictified = hl7_message_to_dict(c, use_long_name=use_long_name)
            if name in d:
                if not isinstance(d[name], list):
                    d[name] = [d[name]]
                d[name].append(dictified)
            else:
                d[name] = dictified
        return d
    else:
        return m.to_er7()


input_files: str = input("Enter location for input HL7 files (including wildcard; ex: /data/dir/*/*.hl7): ")
output_file: str = input("Enter location for output json (ex: /output/dir/converted_files.json: ")
log_file: str = input("Enter location for log file (ex: /log/dir/log.txt): ")

success_count = 0
error_count = 0

log = open(log_file, 'a')
log.write("Beginning processing files in " + input_files + '\n')

for file in glob.glob(input_files):
    with open(os.path.join(os.getcwd(), file), 'rb') as f:
        print("Processing " + file + " ...")
        data = f.read().decode(errors='replace')
        file1 = open(output_file, "a")
        print("Translating " + file + " to dict...")
        try:
            line = hl7_str_to_dict(data)
            print("Successfully translated " + file + " to dict")
        except:
            log.write(file + ",error" + '\n')
            print("Failed to convert " + file + " to dict. Message file may be invalid HL7.")
            error_count += 1
            pass
        line = json.dumps(line)
        file1.write(line)
        file1.write('\n')
        print("Successfully written " + file)
        success_count += 1
        file1.close()

log.write("Total successful: " + str(success_count) + '\n')
log.write("Total errors: " + str(error_count) + '\n')
log.close()


