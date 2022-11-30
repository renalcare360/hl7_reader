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


input_files = "./seghs/*.hl7"
output_file_name = "./output/output.json"
log_file = "./logs/log.txt"

success_count = 0
error_count = 0
message_count = 0

log = open(log_file, 'a')
log.write("Beginning processing files in " + input_files + '\n')

#for each file
for file in glob.glob(input_files):
    with open(os.path.join(os.getcwd(), file), 'rb') as f:

        #read first line
        data = f.readline().decode(errors='replace')
        message_count += 1

        while data:
            file1 = open(output_file_name, "a")

            print("Translating " + str(message_count) + " to dict...")
            
            try:
                line = hl7_str_to_dict(data)
                print("Successfully translated " + str(message_count) + " to dict")
            
            except:
                log.write(str(message_count) + ",error" + '\n')
                print("Failed to convert " + file + " to dict. Message file may be invalid HL7.")
                error_count += 1
                data = f.readline().decode(errors='replace')
                message_count += 1
                pass

            #write to file
            file1.write(json.dumps(line))
            file1.write('\n')

            print("Successfully written " + str(message_count))
            
            #iter
            success_count += 1

            #close output file
            file1.close()

            data = f.readline().decode(errors='replace')
            message_count += 1
            
log.write("Total successful: " + str(success_count) + '\n')
log.write("Total errors: " + str(error_count) + '\n')
log.close()

