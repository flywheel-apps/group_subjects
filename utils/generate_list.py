import pandas as pd
import logging
import itertools as it
import utils.process_config as pc
import numpy as np

import flywheel

log = logging.getLogger(__name__)

fw_error = 'fw_error_value_alpha4324'


def is_query(text):
    # If it starts with "NOT", then it is a query
    if text[:3] == "NOT":
        return (True)
    # If there are no spaces it can't be a query
    if text.find(' ') == -1:
        return (False)

    # If the entire thing is quoted, it's not a query
    subtext, last = pc.text_in_chars(text, '"', '"')
    if len(text) == last + 1:
        return (False)

    # At this point, if we found spaces, and it's not entirely in quotes, we can assume
    # That it's a query, as any fields with spaces in them would need to be quoted.  
    return (True)




def get_unique_values(fw, containers, metadata_key, container_type):
    
    data_dict = data_dict = {'container':[], 'subject':[], 'val':[]}
    
    values = set()
    log.debug('starting')
    for container in containers:
        container = container.reload()
        log.debug(f'container {container.label}')
        log.debug(f'type: {container.container_type}')
        
        try:
            key_tree = metadata_key.split('.')
            sub_level = container
            log.debug(f"key_tree {key_tree}")
            #log.debug(f"sub_level {sub_level}")
            
            for key in key_tree:
                sub_level = sub_level.get(key)
                
            if container_type == 'subject':
                cid = container.id
                subject = container.label
            else:
                cid = container.parents.subject
                subject = fw.get_subject(cid).label
                
            
            value = sub_level
            data_dict['container'].append(cid)
            data_dict['subject'].append(subject)
            data_dict['val'].append(value)
            
        except Exception as e:
            log.exception(e)
            log.warning(f"no object {metadata_key} for {container.id}")
            value = fw_error
            
        log.debug(value)
        if value != fw_error:
            values.add(value)
            
    
    log.debug(values)
    log.debug(data_dict)
    return(values, data_dict)
            
    

def process_subgroups(fw, project, groups, finders=False):
    
    n_groups = 0
    
    prefix = f"{project.group}/{project.label}"
    data_dict = {'container':[], 'subject':[], 'group_num':[], 'group_name':[]}
    group_dict = {}
    for name, text in groups.items():

        
        if is_query(text):

            
            query = f"project._id = {project.id} AND {text}"
            results = fw.search({'structured_query': query, 'return_type': 'session'}, size=10000)
            
            if results:
                n_groups += 1
                group_dict[name] = n_groups
            else:
                log.warning(f'No results for query {query}')
                continue
            
            sessions = [project.sessions.find(f"_id={r.session.id}")[0] for r in results]
            subjects = set()

            # https://stackoverflow.com/questions/10024646/how-to-get-list-of-objects-with-unique-attribute/49168973
            unique_subjects = [subjects.add(ses.subject.id) or ses.subject
                               for ses in sessions
                               if ses.subject.id not in subjects]
            
            for subject in unique_subjects:
                data_dict['container'].append(subject.id)
                data_dict['subject'].append(f"{prefix}/{subject.label}")
                data_dict['group_num'].append(n_groups)
                data_dict['group_name'].append(name)

        else:
            container_level = text.split('.')[0]
            group_key = text[text.find('.')+1:]
            log.debug(f'container_level: {container_level}')
            log.debug(f'group_key: {group_key}')
            
            if container_level == 'subject':
                containers = project.subjects.iter()
            
            if container_level == 'session':
                containers = project.sessions.iter()
            
            if container_level == 'acquisition':
                containers = []
                sessions = project.sessions.iter()
                for ses in sessions:
                    containers.extend(ses.acquisitions.iter())
                containers = it.chain(*containers)
            
            unique_values, sub_dict = get_unique_values(fw, containers, group_key, container_level)
            n_values = len(unique_values)
            
            log.debug(f"unique_values: {unique_values}")
            for val in unique_values:
                n_groups += 1
                group_dict[val] = n_groups
            
            for i in range(len(sub_dict['container'])):
                
                data_dict['container'].append(sub_dict['container'][i])
                data_dict['subject'].append(f"{prefix}/{sub_dict['subject'][i]}")
                data_dict['group_name'].append(sub_dict['val'][i])
                data_dict['group_num'].append(group_dict[sub_dict['val'][i]])
    
    log.debug(data_dict)
    df = pd.DataFrame.from_dict(data_dict)
    return(df)
                

def generate_matrix_from_df(df):
    n_groups = df['group_num'].nunique()
    n_rows = len(df)
    
    ev_mat = np.zeros((n_rows, n_groups))
    group_header = df.sort_values('group_num')['group_name'].unique()
    group_header = [h.replace(' ','_') for h in group_header]
    
    for i in range(n_rows):
        gn = df.iloc[i].group_num
        ev_mat[i, gn-1] = 1
    
    
    con_mat = np.eye(n_groups)
    
    return(ev_mat, con_mat, group_header)

def generate_output_file(df, output_file=None):
    
    df = df.sort_values('group_num').reset_index()
    del df['index']
    
    subject_section = df['subject'].values
    ev_mat, con_mat, group_header = generate_matrix_from_df(df)
    
    subject_section_header = "### SUBJECTS ###"
    ev_section_header = "\n\n### EV MATRIX ###"
    con_section_header = "\n\n### CON MATRIX ###"
    
    if not output_file:
        output_file = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/FSL_Subject_Groupby/group_subjects/output_test.txt'

    with open(output_file, "wb") as f:
        f.write(subject_section_header.encode('utf-8'))
        f.write(b"\n")
        np.savetxt(f, subject_section, fmt='%s')
        
        f.write(ev_section_header.encode('utf-8'))
        f.write(b"\n")
        np.savetxt(f, group_header, fmt='%s', newline=' ')
        f.write(b"\n")
        np.savetxt(f, ev_mat, fmt='%d')
        
        f.write(con_section_header.encode('utf-8'))
        f.write(b"\n")
        np.savetxt(f, group_header, fmt='%s', newline=' ')
        f.write(b"\n")
        np.savetxt(f, con_mat, fmt='%d')
    
    
    
        
        
     



def test():
    fw = flywheel.Client()
    project = fw.get('5db0759469d4f3001f16e9c1')
    #groupstring = '["session.age_in_years" > 30 AND "session.age_in_years" <= 39]:"Decade 30", ["session.age_in_years" > 20 AND "session.age_in_years" <= 29]:"Decade 20", [subject.sex]'
    groupstring = "[subject.sex]:sex,[subject.label]:sbuject"
    groups = pc.split_groups(groupstring)
    
    data_dict = process_subgroups(fw, project, groups)
    df = pd.DataFrame.from_dict(data_dict)
    
