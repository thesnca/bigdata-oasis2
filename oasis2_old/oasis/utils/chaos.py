# -*- coding: utf-8 -*-

'''
这份文件存放 全局/变量/静态资源等一些不知道放哪的方法，类，资源等等
这份文件不引用 oasis项目中其他包，以保证其他包能够引用此文件，而不会导致循环引用。
'''

DISTRIBUTION_SCHEMAS = {
    'KES': {
        'kes-1.0.0': {
            'release': '1.0.0',
            'version': ['OpenSource'],
            # 'main_version': ['5.6.16', '6.8.4', '7.4.2', '7.10.0'],
            'main_version': ['5.6.16', '6.8.4', '7.4.2'],
            'plugins': [
                {
                    'id': 'analysis-ik',
                    'required': True,
                },
                {
                    'id': 'sql',
                    'required': False,
                },
                {
                    'id': 'repository-ks3',
                    'required': False,
                }
            ]
        }
    },
    'KHBASE': {
        'khbase-1.0.0': {
            "release": "1.0.0",
            "version": ["OpenSource"],
            "main_version": ["1.4.12", "2.2.3"]
        }
    }
}

CLUSTER_STATUS_CONVERT_MAP = {
    'VALIDATING': ['Validating'],
    'SPAWNING': ['Spawning'],
    'WAITING': ['Waiting'],
    'PREPARING': ['Preparing'],
    'CONFIGURING': ['Configuring'],
    'STARTING': ['Starting'],
    'RUNNING': ['Active'],
    'FREEZING': ['Freezing'],
    'FREEZE': ['Freeze'],
    'UNFREEZING': ['Unfreezing'],
    'TERMINATING': ['Deleting'],
    'TERMINATED': ['Deleted'],
    'TERMINATED_WITH_ERRORS': ['Error'],
    'PROGRESSING': ['Progressing'],
    'UPGRADING': ['Upgrading'],
    'SCALEOUTVALIDATING': ['ScaleOutValidating'],
    'SCALEOUTWAITING': ['ScaleOutWaiting'],
    'SCALEOUTPREPARING': ['ScaleOutPreparing'],
    'SCALEOUTCONFIGURING': ['ScaleOutConfiguring'],
    'SCALEOUTSTARTING': ['ScaleOutStarting'],
    'SCALEINVALIDATING': ['ScaleInValidating'],
    'SCALEINPREPARING': ['ScaleInPreparing'],
    'SCALEINWAITING': ['ScaleInWaiting'],
    'SCALEINCONFIGURING': ['ScaleInConfiguring'],
    'SCALEINSTARTING': ['ScaleInStarting'],
    'RESTARTING': ['Restarting'],
    'NONDELETED': ['NonDeleted'],
    'ALL': ['All']
}

OP_CLUSTER_STATUS_CONVERT_MAP = {
    'CREATING': ['Validating', 'Spawning', 'Waiting', 'Preparing', 'Configuring', 'Starting'],
    'RUNNING': ['Active'],
    'PROGRESSING': ['Freezing', 'Unfreezing', 'Deleting', 'Progressing', 'Upgrading', 'ScaleOutValidating',
                    'ScaleOutWaiting', 'ScaleOutPreparing', 'ScaleOutConfiguring', 'ScaleOutStarting', 'Restarting'],
    'FREEZE': ['Freeze'],
    'TERMINATED': ['Deleted'],
    'TERMINATED_WITH_ERRORS': ['Error'],
    'NONDELETED': ['NonDeleted'],
    'ALL': ['All']
}
