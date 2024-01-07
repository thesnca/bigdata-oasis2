from oasis.db.models.es_plugin import EsPluginModel
from oasis.utils.convert import datetime2str


class KesPlugin(object):

    system_plugin_results = [
        {
            'PluginId': '1',
            'Name': 'analysis-ik',
            'PluginType': 0,
            'Status': 1,
            'Description': 'Elasticsearch IK 分析插件，默认不能卸载。',
            'Ks3Address': '',
            'CreateTime': '',
            'UpdateTime': ''
        },
        {
            'PluginId': '2',
            'Name': 'sql',
            'PluginType': 0,
            'Status': 1,
            'Description': 'SQL查询插件，默认不能卸载。',
            'Ks3Address': '',
            'CreateTime': '',
            'UpdateTime': ''
        },
        {
            'PluginId': '3',
            'Name': 'KS3',
            'PluginType': 0,
            'Status': 1,
            'Description': 'KS3插件，默认不能卸载。',
            'Ks3Address': '',
            'CreateTime': '',
            'UpdateTime': ''
        },
    ]

    def __init__(self, plugin: EsPluginModel = None):
        self.PluginId = None
        self.Name = None
        self.PluginType = None
        self.Status = None
        self.Description = None
        self.Ks3Addr = None

        if plugin is not None:
            self.convert_from(plugin)

    def convert_from(self, plugin: EsPluginModel):
        self.PluginId = plugin.id
        self.Name = plugin.name
        self.PluginType = plugin.plugin_type
        self.Status = plugin.status
        self.Description = plugin.description
        self.Ks3Address = plugin.ks3_address

        created_at = datetime2str(plugin.created_at)
        updated_at = datetime2str(plugin.updated_at)
        if created_at:
            self.CreateTime = created_at.replace('T', ' ')
        else:
            self.CreateTime = created_at
        if updated_at:
            self.UpdateTime = updated_at.replace('T', ' ')
        else:
            self.UpdateTime = updated_at
        
