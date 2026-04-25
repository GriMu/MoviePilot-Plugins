import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote
import random

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.utils.http import RequestUtils
from app.core.config import settings
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional
from app.log import logger
import xml.dom.minidom
from app.utils.dom import DomUtils


def retry(ExceptionToCheck: Any,
          tries: int = 3, delay: int = 3, backoff: int = 1, logger: Any = None, ret: Any = None):
    """重试装饰器，支持自定义请求头"""

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"未获取到文件信息，{mdelay}秒后重试 ... 错误: {str(e)[:100]}"
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if logger:
                logger.warn('请确保当前季度番剧文件夹存在或检查网络问题')
            return ret

        return f_retry

    return deco_retry


class OpenANi(_PluginBase):
    # 插件名称
    plugin_name = "OpenANi"
    # 插件描述
    plugin_desc = "自动获取当季所有番剧，支持自定义请求头绕过Cloudflare防护"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    # 插件版本
    plugin_version = "2.5.0"
    # 插件作者
    plugin_author = "GriMu"
    # 作者主页
    author_url = "https://github.com/GriMu"
    # 插件配置项ID前缀
    plugin_config_prefix = "openani_"
    # 加载顺序
    plugin_order = 15
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _enabled = False
    _cron = None
    _onlyonce = False
    _fulladd = False
    _storageplace = None
    
    # 自定义配置属性
    _custom_season = None
    _custom_rss_url = None
    _custom_season_url = None
    _custom_headers = None  # 新增：自定义请求头
    
    # 默认镜像列表（用于轮询）
    DEFAULT_MIRRORS = [
        'https://openani.an-i.workers.dev',
        'https://pili.cc.cd',
        'https://ani.v300.eu.org',
        'https://ani.op5.de5.net',
    ]
    
    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._fulladd = config.get("fulladd")
            self._storageplace = config.get("storageplace")
            self._custom_season = config.get("custom_season")
            self._custom_rss_url = config.get("custom_rss_url")
            self._custom_season_url = config.get("custom_season_url")
            self._custom_headers = config.get("custom_headers")  # 加载自定义请求头
            
        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(func=self.__task,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="OpenANi文件创建")
                    logger.info(f'OpenANi定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")

            if self._onlyonce:
                logger.info(f"OpenANi服务启动，立即运行一次")
                self._scheduler.add_job(func=self.__task, args=[self._fulladd], trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="OpenANi文件创建")
                self._onlyonce = False
                self._fulladd = False
            self.__update_config()

            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        """获取季度，优先使用自定义值"""
        if self._custom_season:
            return self._custom_season
            
        current_date = datetime.now()
        current_year = current_date.year
        current_month = idx_month if idx_month else current_date.month
        for month in range(current_month, 0, -1):
            if month in [10, 7, 4, 1]:
                self._date = f'{current_year}-{month}'
                return f'{current_year}-{month}'
        return f'{current_year}-1'

    def __get_headers(self) -> dict:
        """获取请求头，优先使用自定义请求头"""
        # 如果用户配置了自定义请求头，优先使用
        if self._custom_headers:
            try:
                # 解析用户输入的请求头（格式：Key: Value，每行一个）
                headers = {}
                for line in self._custom_headers.strip().split('\n'):
                    if ':' in line:
                        key, value = line.split(':', 1)
                        headers[key.strip()] = value.strip()
                if headers:
                    logger.info("使用自定义请求头")
                    return headers
            except Exception as e:
                logger.warning(f"解析自定义请求头失败: {e}")
        
        # 默认浏览器请求头（模拟真实浏览器）
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        
        # 随机化部分请求头，增加真实性
        default_headers['User-Agent'] = random.choice([
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        ])
        
        return default_headers

    @retry(Exception, tries=5, delay=5, logger=logger, ret=[])
    def get_current_season_list(self) -> List:
        """获取当前季度番剧列表，支持多镜像轮询"""
        season = self.__get_ani_season()
        
        # 构建镜像列表
        if self._custom_season_url:
            mirrors = [self._custom_season_url]
        else:
            mirrors = self.DEFAULT_MIRRORS.copy()
            random.shuffle(mirrors)  # 随机化顺序
        
        headers = self.__get_headers()
        
        # 尝试每个镜像
        for mirror in mirrors:
            try:
                url = f'{mirror.rstrip("/")}/{season}/'
                logger.info(f'尝试镜像: {url}')
                
                # 使用自定义请求头
                rep = RequestUtils(
                    headers=headers,
                    ua=settings.USER_AGENT if settings.USER_AGENT else None,
                    proxies=settings.PROXY if settings.PROXY else None
                ).post(url=url)
                
                # 检查响应状态
                if rep.status_code == 200:
                    try:
                        data = rep.json()
                        if 'files' in data:
                            files_json = data['files']
                            logger.info(f'成功获取 {len(files_json)} 个文件，镜像: {mirror}')
                            return [file['name'] for file in files_json]
                    except Exception as e:
                        logger.warning(f'JSON解析失败: {e}')
                elif rep.status_code == 404:
                    logger.warning(f'镜像 {mirror} 不存在季度 {season}')
                    continue
                else:
                    logger.warning(f'镜像 {mirror} 返回状态码: {rep.status_code}')
                    
            except Exception as e:
                logger.warning(f'镜像 {mirror} 请求失败: {e}')
                continue
        
        # 所有镜像都失败
        logger.error(f'所有镜像均无法获取季度 {season} 的文件列表')
        return []

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        """获取最新更新列表（RSS）"""
        addr = self._custom_rss_url if self._custom_rss_url else 'https://api.ani.rip/ani-download.xml'
        headers = self.__get_headers()
        
        try:
            ret = RequestUtils(
                headers=headers,
                ua=settings.USER_AGENT if settings.USER_AGENT else None,
                proxies=settings.PROXY if settings.PROXY else None
            ).get_res(addr)
            
            if ret.status_code != 200:
                logger.warning(f'RSS源返回状态码: {ret.status_code}')
                return []
                
            ret_xml = ret.text
            ret_array = []
            
            # 解析XML
            dom_tree = xml.dom.minidom.parseString(ret_xml)
            rootNode = dom_tree.documentElement
            items = rootNode.getElementsByTagName("item")
            
            for item in items:
                rss_info = {}
                title = DomUtils.tag_value(item, "title", default="")
                link = DomUtils.tag_value(item, "link", default="")
                
                # 如果用户自定义了域名，进行替换适配
                if self._custom_season_url and link:
                    if "resources.ani.rip" in link:
                        link = link.replace("resources.ani.rip", "openani.an-i.workers.dev")
                
                rss_info['title'] = title
                rss_info['link'] = link
                ret_array.append(rss_info)
                
            return ret_array
            
        except Exception as e:
            logger.error(f'获取RSS列表失败: {e}')
            return []

    def __touch_strm_file(self, file_name, file_url: str = None) -> bool:
        """创建strm文件"""
        if not file_url:
            # 季度API生成的URL
            base_url = self._custom_season_url if self._custom_season_url else 'https://openani.an-i.workers.dev'
            encoded_filename = quote(file_name, safe='')
            src_url = f'{base_url.rstrip("/")}/{self._date}/{encoded_filename}.mp4?d=true'
        else:
            # RSS获取的URL，确保格式正确
            if self._is_url_format_valid(file_url):
                src_url = file_url
            else:
                src_url = self._convert_url_format(file_url)
        
        file_path = f'{self._storageplace}/{file_name}.strm'
        if os.path.exists(file_path):
            logger.debug(f'{file_name}.strm 文件已存在')
            return False
            
        try:
            with open(file_path, 'w') as file:
                file.write(src_url)
                logger.debug(f'创建 {file_name}.strm 文件成功')
                return True
        except Exception as e:
            logger.error(f'创建strm源文件失败: {e}')
            return False

    def _is_url_format_valid(self, url: str) -> bool:
        """检查URL格式是否符合要求"""
        return url.endswith('.mp4?d=true')

    def _convert_url_format(self, url: str) -> str:
        """将URL转换为符合要求的格式"""
        if '?d=mp4' in url:
            return url.replace('?d=mp4', '.mp4?d=true')
        elif url.endswith('.mp4'):
            return f'{url}?d=true'
        else:
            return f'{url}.mp4?d=true'

    def __task(self, fulladd: bool = False):
        """核心任务执行"""
        cnt = 0
        
        if not fulladd:
            # 增量添加
            rss_info_list = self.get_latest_list()
            logger.info(f'本次处理 {len(rss_info_list)} 个RSS文件')
            for rss_info in rss_info_list:
                if self.__touch_strm_file(file_name=rss_info['title'], file_url=rss_info['link']):
                    cnt += 1
        else:
            # 全量添加
            name_list = self.get_current_season_list()
            logger.info(f'本次处理 {len(name_list)} 个季度文件')
            for file_name in name_list:
                if self.__touch_strm_file(file_name=file_name):
                    cnt += 1
                    
        logger.info(f'新创建了 {cnt} 个strm文件')

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """插件配置页面"""
        return [
            {
                'component': 'VForm',
                'content': [
                    # 基础配置行
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}
                            ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}
                            ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VSwitch', 'props': {'model': 'fulladd', 'label': '下次创建当前季度所有番剧strm'}}
                            ]}
                        ]
                    },
                    # 路径和周期配置行
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期', 'placeholder': '0 0 ? ? ?'}}
                            ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'storageplace', 'label': 'Strm存储地址', 'placeholder': '/downloads/strm'}}
                            ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'custom_season', 'label': '自定义季度 (可选)', 'placeholder': '如: 2024-1'}}
                            ]}
                        ]
                    },
                    # 自定义链接配置行
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'custom_rss_url', 'label': '自定义RSS订阅源 (可选)', 'placeholder': '留空使用默认ani.rip RSS源'}}
                            ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'custom_season_url', 'label': '自定义季度API基础链接 (可选)', 'placeholder': '留空使用默认openani Workers链接'}}
                            ]}
                        ]
                    },
                    # 新增：自定义请求头配置
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                                {
                                    'component': 'VTextarea',
                                    'props': {
                                        'model': 'custom_headers',
                                        'label': '自定义请求头 (可选，高级用户)',
                                        'placeholder': '每行一个，格式: Key: Value\n例如:\nUser-Agent: Mozilla/5.0\nReferer: https://openani.an-i.workers.dev/',
                                        'rows': 6,
                                        'hint': '留空使用内置的浏览器模拟请求头。此功能用于绕过Cloudflare等防护。'
                                    }
                                }
                            ]}
                        ]
                    },
                    # 说明信息
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                                {
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': '✨ **新功能**：支持自定义请求头，有效绕过Cloudflare 1101错误\n'
                                                '🔧 **改进**：内置多镜像轮询机制，自动尝试可用镜像站\n'
                                                '⚡ **优化**：随机化User-Agent，模拟真实浏览器访问\n\n'
                                                '自动从API抓取下载直链生成strm文件，免去人工订阅下载\n'
                                                '配合目录监控使用，strm文件创建在存储地址，mp会完成刮削',
                                        'style': 'white-space: pre-line;'
                                    }
                                },
                                {
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'warning',
                                        'variant': 'tonal',
                                        'text': '⚠️ **注意**：如果所有镜像都失败，请检查网络和代理设置\n'
                                                '💡 **建议**：优先配置自定义请求头，通常可解决1101错误\n'
                                                '🔄 **镜像**：内置多个镜像站，会自动轮询尝试',
                                        'style': 'white-space: pre-line;'
                                    }
                                }
                            ]}
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "fulladd": False,
            "storageplace": '/downloads/strm',
            "cron": "*/20 22,23,0,1 * * *",
            "custom_season": "",
            "custom_rss_url": "",
            "custom_season_url": "",
            "custom_headers": ""  # 新增默认值
        }

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "storageplace": self._storageplace,
            "custom_season": self._custom_season,
            "custom_rss_url": self._custom_rss_url,
            "custom_season_url": self._custom_season_url,
            "custom_headers": self._custom_headers  # 持久化自定义请求头
        })

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """退出插件"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"退出插件失败: {e}")


if __name__ == "__main__":
    # 测试代码
    plugin = OpenANi()
    
    # 模拟配置
    test_config = {
        "enabled": True,
        "custom_headers": "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\nReferer: https://openani.an-i.workers.dev/"
    }
    
    plugin.init_plugin(test_config)
    
    # 测试获取列表
    print("测试获取季度列表...")
    season_list = plugin.get_current_season_list()
    print(f"获取到 {len(season_list)} 个文件")
    
    print("\n测试获取RSS列表...")
    rss_list = plugin.get_latest_list()
    print(f"获取到 {len(rss_list)} 个RSS条目")
