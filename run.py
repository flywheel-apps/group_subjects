import logging
import re
import sys

import flywheel
import flywheel_gear_toolkit


import utils.process_config as pc
import utils.generate_list as gl
log = logging.getLogger(__name__)


def main(fw, gear_context):
    
    config = gear_context.config
    groups = pc.parse_groups(config)

    destination = gear_context.destination["id"]
    analysis = fw.get(destination)
    project = fw.get_project(analysis.parents.project)
     
    subgroup_df = gl.process_subgroups(fw, project, groups, finders=False)
    log.debug(subgroup_df)
    output_file = '/flywheel/v0/output/subject_groups.txt'
    gl.generate_output_file(subgroup_df, output_file)
    return(0)

if __name__ == "__main__":


    
    with flywheel_gear_toolkit.GearToolkitContext(config_path='/flywheel/v0/config.json') as gear_context:
        gear_context.config['debug_gear'] = True
        gll = gear_context.config['Gear Log Level']
        gear_context.init_logging(gll)
        gear_context.log_config()

        fw = flywheel.Client(gear_context.get_input("api_key")["key"])
        
        exit_status = main(fw, gear_context)
            
    if exit_status != 0:
        log.error('Failed')
        sys.exit(exit_status)
        
    log.info(f"Report generated successfully {exit_status}.")
