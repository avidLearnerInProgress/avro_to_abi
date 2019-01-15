# Author: Chirag Shah <avidLearnerInProgress> <chiragshah9696@gmail.com>
# Modified on: 16-01-19 03:08:34 
# Purpose: Avro specific dml ---> abinitio intermediate dml

import re, time, os
#---------------------------Helpers--------------------

#reads line by line 
def read_lines(fname):
    if fname is None:
        return None
    else:
        with open(fname) as f:
            lines = f.read()
        return lines

#picks the string block from the source avro file
#requires start and end sub-string for the block
#writes the block of code to other intermediate file
def preprocess_avro(fread, ifile, start, end):
    if fread is None or ifile is None or start is None or end is None:
        exit()
    
    buffer = ""
    log = False
    for line in open(fread):
        if line.startswith(start):
           # buffer += line
            log = True
        elif line.startswith(end):
           # buffer += line
            log = False
        elif log:
            buffer += line
    open(ifile, 'w').write(buffer)
    if log == True:
        print("End string was not found")

#picks the string block from the source abinitio file
#requires start and end sub-string for the block
#writes the block of code to other intermediate file
def preprocess_abinitio(fread, ifile, start, end):
    if fread is None or ifile is None or start is None or end is None:
        exit()

    buffer = ""
    log = False
    for line in open(fread):
        if line.startswith(start):
           # buffer = line
            log = True
        elif line.startswith(end):
           # buffer += line
            log = False
        elif log:
            buffer += line
    
    open(ifile, 'a').write(buffer)
    if log == True:
        print("End string was not found")

#process the interim avro file
def control_interm_avro(fname):
    if fname is None:
        return None
    
    lol = []
    with open(fname) as f1:
        for line in f1:
            line = line.replace("=","").replace(";","").replace("1","").strip()
            lol.append(re.findall(r'\s?(\s*\S+)', line.rstrip())[::-1])
    #print(lol)
    return lol

#process the interim abinitio file
def control_interm_abinitio(fname):
    if fname is None:
        return None
    
    lol = []
    with open(fname) as f1:
        for line in f1:
            line = line.replace("    ", " ").replace(' NULL("")', 'NULL').replace("=", "").replace(";", "").strip()
            lol.append(re.findall(r'\s?(\s*\S+)', line.rstrip())[::-1])
    return lol


#final call which writes data to the dml file
def logging(mymap, abinitio_dict, abinitio_list):
    buffer = 'record\n'
    RESULT_FILE = 'result.dml'
    if mymap is None or abinitio_dict is None or abinitio_list is None:
        return None
    print("I think I am done, bored :|")
    print("\n\n---------End of Script. @#$%^&--------\n\n")    

    for k, v in mymap.items():
        for ele in abinitio_list:
            if k == ele[1] and ele[0] == 'NULL':
                buffer += v + " " + k + " = " + 'NULL("");' + '\n'
            if (k == ele[1] or k == ele[0]) and ele[0] != 'NULL':
                #print(v + " | " + k )
                buffer += v + " " + k + ';' + '\n'
    buffer += 'end;'
    with open(RESULT_FILE, 'w') as f:
        f.write(buffer)

#performs final mapping of k,v's
def keying(abinitio_dict, avro_dict, abinitio_list):
    if abinitio_dict is None or avro_dict is None or abinitio_list is None:
        return None
    print("Final fields for dml..")

    final_map = dict()
    for k,v in avro_dict.items():
        if k in abinitio_dict:
            print(k + "--> " + abinitio_dict[k])
            final_map[k] = abinitio_dict[k]
    logging(final_map, abinitio_dict, abinitio_list)

#merge the output of both lists
def merge(abinitio_list, avro_list):    
    if abinitio_list is None or avro_list is None:
        return None
    
    date = "datetime('YYYY-MM-DD HH24:MI:SS')"
    abinitio_dict, avro_dict = dict(), dict()
    for ele in abinitio_list:
        if len(ele) > 2:
            if "HH24:MI:SS" in ele[2]:
                #print(ele[1] + "-->" + date)
                abinitio_dict[ele[1]] = date
            elif ele[0] == 'NULL':
                #print(ele[1] + "-->" + ele[2])
                abinitio_dict[ele[1]] = ele[2]
            else:
                #print(ele[0] + "-->" + ele[1])
                abinitio_dict[ele[0]] = ele[1]
        else:
            if ele[0] == 'NULL':
                #print(ele[1] + "-->" + ele[2])
                abinitio_dict[ele[1]] = ele[2]
            else:
                #print(ele[0] + "-->" + ele[1])
                abinitio_dict[ele[0]] = ele[1]
    
    print("\n\n_________\n\n")
    for ele in avro_list:
        curr = ele[::-1]
        avro_dict[curr[1]] = curr[0]
        
    print(abinitio_dict)
    print("\n\n---------\n\n")
    print(avro_dict)
    keying(abinitio_dict, avro_dict, abinitio_list)


#main call which processes the source files
def process(f1, f2):
    avro_lines = read_lines(f1)
    abinitio_lines = read_lines(f2)
    
    dir_path = (os.path.dirname(os.path.realpath(__file__)))
    os.remove(dir_path + "\\" + "interm_avro.txt")
    os.remove(dir_path + "\\" + "interm_abinitio.txt")

    avro_start_ptr, avro_end_ptr  = "type Record_0_t = record" , "end;"
    abinitio_start_ptr1, abinitio_end_ptr1 = "type new_record = record", "end;"
    abinitio_start_ptr2, abinitio_end_ptr2 = "metadata_type = record", "end;"

    preprocess_avro(f1, "interm_avro.txt", avro_start_ptr, avro_end_ptr)
    preprocess_abinitio(f2, "interm_abinitio.txt", abinitio_start_ptr1, abinitio_end_ptr1)
    preprocess_abinitio(f2, "interm_abinitio.txt", abinitio_start_ptr2, abinitio_end_ptr2)
    
    time.sleep(1)
    avro_list = control_interm_avro("interm_avro.txt")
    time.sleep(1)
    abinitio_list = control_interm_abinitio("interm_abinitio.txt")
    merge(abinitio_list, avro_list)
    
#entry point
if __name__ == '__main__':
    AVRO_FILE = "avro.txt"
    ABINITO_FILE = "abintio.txt"
    process(AVRO_FILE, ABINITO_FILE)