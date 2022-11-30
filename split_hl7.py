with open('seghs\renalCare360ADT.hl7', 'rb') as big_file:
    message_id = 1
    with open('seghs/split_files/' + str(message_id) + 'hl7', 'xb') as out_file:
        out_file.write(out_file)