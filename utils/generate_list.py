import pandas as pd
import logging
import itertools as it
import utils.process_config as pc
import numpy as np

import flywheel
import utils.flywheel_helpers as fh

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




def get_unique_values(fw, containers, metadata_key, level):
    
    data_dict =  {'container':[], 'val':[]}
    
    unq_values = set()
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
                
            value = sub_level

            containers = fh.get_containers_at_level(fw, container, level)
            values = [value]*len(containers)
            data_dict['container'].extend(containers)
            data_dict['val'].extend(values)
            
        except Exception as e:
            log.exception(e)
            log.warning(f"no object {metadata_key} for {container.id}")
            value = fw_error
            
        log.debug(value)
        if value != fw_error:
            unq_values.add(value)
            

    return(unq_values, data_dict)
            

def run_query_for_containers(fw, project, text, level='session'):
    
    if level == 'subject' or level == 'project':
        query_level = 'session'
    else:
        query_level = level

    query = f'project._id = {project.id} AND {text}'
    log.debug(query)
    log.debug(query_level)
    results = fw.search({'structured_query': query, 'return_type': query_level}, size=10000)
    
    return(results, query_level)
    
    


def unique_containers_from_list(containers):
    
    level_set = set()
    # https://stackoverflow.com/questions/10024646/how-to-get-list-of-objects-with-unique-attribute/49168973
    unique_containers = [level_set.add(container.id) or container
                         for container in containers
                         if container.id not in level_set]
    
    return(unique_containers)
    
    
def set_subgroup_to_level(fw, containers, level):
    
    test_c = containers[0]
    if test_c.container_type is level:
        return (containers)
    
    new_containers = []
    
    for container in containers:
        new_containers.extend(fh.get_containers_at_level(fw, container, level))
    
    new_containers = unique_containers_from_list(new_containers)
    
    return (new_containers)
    

def build_subgroup_dict(fw, containers, name):
    
    #unique_containers = unique_containers_from_list(containers)
    ids = [c.id for c in containers]
    
    paths = []
    group = None
    project = None
    for c in containers:
        fw_path = fh.generate_path_to_container(fw, c, group=group, project=project)
        if group is None:
            split_path = fw_path.split('/')
            group = split_path[0]
            project = split_path[1]
        paths.append(fw_path)
    
    data_dict = {'container': ids, 'path': paths, 'name': name}
    
    return(data_dict)
    
    #### Next step:  Take multiple dicts like this and concat them all together, each one a new group.  Build up DF


def make_evs_fom_dicts(group_dicts):
    
    full_id_list = []
    n_groups = len(group_dicts)
    full_path_list = []
    
    for dict in group_dicts:
        log.debug(dict)
        ids = dict['container']
        paths = dict['path']
        for fwid, fwpath in zip(ids, paths):
            if fwid not in full_id_list:
                full_id_list.append(fwid)
                full_path_list.append(fwpath)
    
    
    
    n_unique = len(full_id_list)
    
    full_dict = {'containers': full_id_list, 'paths': full_path_list}
    
    
    for ng, dict in enumerate(group_dicts):
        containers = dict['container']
        name = dict['name']
        grouping = np.zeros(n_unique)
        for fwid in full_id_list:
            if fwid in containers:
                i = full_id_list.index(fwid)
                grouping[i] = 1
        
        group_name = f"g{ng}_{name}"
        full_dict[group_name] = grouping
    
    return(full_dict)
        

def unique_vals_to_subdicts(fw, values, data_dict, level):
    #{'container': [], 'val': []}
    containers = np.array(data_dict['container'])
    data_vals = np.array(data_dict['val'])
    data_dicts = []
    for val in values:
        log.debug(val)
        log.debug(data_vals)
        inds = np.where(data_vals == val)
        log.debug(inds)
        sub_containers = containers[inds]
        new_containers = unique_containers_from_list(sub_containers)

        ids = [c.id for c in new_containers]
    
        paths = []
        group = None
        project = None
        for c in new_containers:
            fw_path = fh.generate_path_to_container(fw, c, group=group, project=project)
            if group is None:
                split_path = fw_path.split('/')
                group = split_path[0]
                project = split_path[1]
            paths.append(fw_path)
    
        data_dict = {'container': ids, 'path': paths, 'name': val}
        data_dicts.append(data_dict)
        
    return(data_dicts)
        
        
        
    
    
    

def process_subgroups(fw, project, groups, level='subject', finders=False):
    
    n_groups = 0
    
    prefix = f"{project.group}/{project.label}"
    data_dict = {'container': [], 'path': [], 'group_num': [], 'group_name':[]}
    group_dicts = []
    for name, text in groups.items():
        
        if is_query(text):
            
            results, ql = run_query_for_containers(fw, project, text, level)
            
            if not results:
                log.warning(f'No results for query {text}')
                continue
            
            containers = [fh.get_level(fw, r.get(ql).id, ql) for r in results]
            
            containers = set_subgroup_to_level(fw, containers, level)
            subgroup_dict = build_subgroup_dict(fw, containers, name)
            group_dicts.append(subgroup_dict)


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
            
            unique_values, sub_dict = get_unique_values(fw, containers, group_key, level)
            subgroup_dict = unique_vals_to_subdicts(fw, unique_values, sub_dict, level)
            group_dicts.extend(subgroup_dict)

    full_dict = make_evs_fom_dicts(group_dicts)
    log.debug(full_dict)
    df = pd.DataFrame.from_dict(full_dict)
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
    
def generate_matrix_from_df2(df):
    n_groups = len(df.columns) - 2
    n_rows = len(df)
    
    group_header = list(df.columns[2:])
    
    ev_mat = np.zeros((n_rows, n_groups))
    
    for ih, header in enumerate(group_header):
        ev_mat[:, ih] = df[header].values
        
    group_header = [h.replace(' ', '_') for h in group_header]

    con_mat = np.eye(n_groups)
    
    return(ev_mat, con_mat, group_header)

def generate_output_file(df, output_file=None):
    
    # df = df.sort_values('group_num').reset_index()
    # del df['index']
    
    subject_section = df['paths'].values
    ev_mat, con_mat, group_header = generate_matrix_from_df2(df)
    
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
    groupstring = '[subject.sex], ["session.age_in_years" > 30]:"old"'
    groups = pc.split_groups(groupstring)
    
    df = process_subgroups(fw, project, groups, level='session', finders=False)

    return(df)
