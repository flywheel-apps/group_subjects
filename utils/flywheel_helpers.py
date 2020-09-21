
def get_children(container):
    
    ct = container.container_type
    if ct == 'project':
        children = container.subjects()
    elif ct == 'subject':
        children = container.sessions()
    elif ct == 'session':
        children = container.acquisitions()
    elif ct == 'acquisition' or ct == 'analysis':
        children = container.files
    else:
        children = []
        
    return(children)

def get_parent(fw, container):
    
    ct = container.container_type

    if ct == 'project':
        parent = fw.get_group(container.group)
    elif ct == 'subject':
        parent = fw.get_project(container.project)
    elif ct == 'session':
        parent = container.subject
    elif ct == 'acquisition':
        parent = container.get_session(container.session)
    elif ct == 'analysis':
        parent = fw.get(container.parent['id'])
    else:
        parent = None
        
    return(parent)
