import logging

log=logging.getLogger(__name__)



def text_in_chars(text,start,stop):
    
    if start == '"' and stop == '"':
        first = text.find('"')
        last = text[first+1:].find('"')
        substring = text[first+1:last+first+1]
        return(substring, last+first+1)
    
    first = text.find(start)
    if first == -1:
        return('',-1)
    
    count = 0
    last = -1
    for i in range(first,len(text)):
        
        if text[i] == start:
            count+=1
        elif text[i] == stop:
            count-=1
        if count == 0:
            last = i
            break
            
    if last == -1:
        log.warning(f'No closing character {stop}')
        substring = ''
    else:
        substring = text[first+1:last]
        
    return(substring, last)



def split_groups(group_string):
    
    start = '['
    end = ']'
    
    if group_string[0] != start:
        log.error('Groups must be in square brackets "[...]", EVEN if there is only one group')
        raise Exception('Invalid Input')
    
    process_subgroups = True
    subgroups = {}
    group_count = 0
    
    while process_subgroups:
        subgroup_string, last_index = text_in_chars(group_string, start, end)

        if subgroup_string == '':
            process_subgroups = False
            
        else:
            group_count += 1
            group_string = group_string[last_index+1:]
            
            if group_string != '' and group_string[0] == ':':
                subgroup_name, last_index = text_in_chars(group_string, '"', '"')
                last_index = last_index+1
                
            else:
                subgroup_name = f"group_{group_count}"
                last_index = 0
                
            subgroups[subgroup_name] = subgroup_string
            group_string = group_string[last_index:]
    
    return(subgroups)
            
            
                


def parse_groups(config):
    
    group_string = config['Group by']
    subgroups = split_groups(group_string)
    return(subgroups)
    # syntax:
    # ["info.age" > 30 AND "info.age" <= 39]:"Decade 30"$["info.age" > 20 AND "info.age" <= 29]:"Decade 20"
    # ["subject.sex"] - automatically makes one group for each sex
    # ["subject.cohort" = control]:"control",["subject.cohort" = group1]:"Patients"


def test():
    
    groupstring = '["info.age" > 30 AND "info.age" <= 39]:"Decade 30", ["info.age" > 20 AND "info.age" <= 29]:"Decade 20", [subject.sex]'
    groups = split_groups(groupstring)