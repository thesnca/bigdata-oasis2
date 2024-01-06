from oasis.utils.sdk.charging.charge_platform import ChargeClient
from oasis.utils.sdk.charging.price_platform import PriceClient
from oasis.utils.sdk.charging.product_platform import ProductClient
from oasis.utils.sdk.feishu import FeishuClient
from oasis.utils.sdk.gringotts.monitor import GringottsMonitorClient
from oasis.utils.sdk.gringotts.server import GringottsClient
from oasis.utils.sdk.infra.eagles import EaglesClient
from oasis.utils.sdk.infra.ebs import EbsClient
from oasis.utils.sdk.infra.eip import EipClient
from oasis.utils.sdk.infra.epc import EpcClient
from oasis.utils.sdk.infra.kec import KecClient
from oasis.utils.sdk.infra.ks3 import Ks3Client
from oasis.utils.sdk.infra.neutron import Neutron
from oasis.utils.sdk.platform.tag import TagClient
from oasis.utils.sdk.infra.sks import SksClient
from oasis.utils.sdk.infra.slb import SLBClient
from oasis.utils.sdk.infra.vpc import VpcClient

# TODO init client as needed
kec_client_kes = KecClient('kes')
kec_client_khbase = KecClient('khbase')
epc_client_kes = EpcClient('kes')
epc_client_khbase = EpcClient('khbase')
sks_client_kes = SksClient('kes')
sks_client_khbase = SksClient('khbase')
vpc_client_kes = VpcClient('kes')
vpc_client_khbase = VpcClient('khbase')
eip_client_kes = EipClient('kes')
eip_client_khbase = EipClient('khbase')
slb_client_kes = SLBClient('kes')
slb_client_khbase = SLBClient('khbase')
ebs_client_kes = EbsClient('kes')
ebs_client_khbase = EbsClient('khbase')
tag_client_kes = TagClient('kes')
tag_client_khbase = TagClient('khbase')

price_client = PriceClient()
charge_client = ChargeClient()
product_client = ProductClient()

ks3_client = Ks3Client()
neutron_client = Neutron()
eagles_client = EaglesClient()
gringotts_client = GringottsClient()
gringotts_monitor_client = GringottsMonitorClient()
feishu_client = FeishuClient()
