import os
import time
import json
from datetime import datetime, timedelta
from urllib.parse import quote

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
    """重试装饰器"""
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
    # 插件基本信息
    plugin_name = "OpenANi"
    plugin_desc = "自动获取当季所有番剧，支持目录遍历和文件直链生成"
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "2.5.0"
    plugin_author = "GriMu"
    author_url = "https://github.com/GriMu"
    plugin_config_prefix = "openani_"
    plugin_order = 15
    auth_level = 2

    # 私有属性
    _enabled = False
    _cron = None
    _onlyonce = False
    _fulladd = False
    _storageplace = None
    _custom_season = None
    _custom_rss_url = None
    _custom_season_url = None
    _custom_headers = None
    _current_base_url = None  # 当前使用的镜像基础URL
    
    # 必要的请求头模板（基于您的成功请求）
    def _get_necessary_headers(self) -> dict:
        """获取必要的请求头，自动适配当前镜像"""
        base_url = self._current_base_url or 'https://openani.an-i.workers.dev'
        season = self.__get_ani_season()
        
        return {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': base_url,
            'Pragma': 'no-cache',
            'Priority': 'u=1, i',
            'Sec-Ch-Ua': '"Microsoft Edge";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'X-Requested-With': 'XMLHttpRequest',
        }

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
            self._custom_headers = config.get("custom_headers")
            
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

    def __make_api_request(self, url: str, headers: dict = None, post_data: str = '{"password":"null"}') -> Optional[Dict]:
        """
        统一的API请求方法
        返回解析后的JSON字典，失败返回None
        """
        if not headers:
            headers = self._get_necessary_headers()
        
        # 更新Referer为当前请求的URL
        headers['Referer'] = url
        
        try:
            response = RequestUtils(
                headers=headers,
                proxies=settings.PROXY if settings.PROXY else None,
                timeout=30
            ).post(url=url, data=post_data)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data
                except ValueError as json_err:
                    logger.error(f'JSON解析失败: {json_err}')
                    logger.debug(f'响应内容前200字符: {response.text[:200]}')
                    return None
            elif response.status_code == 403:
                logger.error(f'禁止访问 (403)，可能需要有效密码')
                return None
            elif response.status_code == 500:
                logger.error(f'服务器错误 (500)')
                return None
            else:
                logger.warning(f'返回状态码: {response.status_code}')
                return None
                
        except Exception as e:
            logger.error(f'API请求失败: {e}')
            return None

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_current_season_list(self) -> List[Dict]:
        """
        获取当前季度番剧列表
        兼容处理文件列表和目录列表两种返回格式
        返回格式: [{"name": "文件名/目录名", "id": "ID", "is_folder": False}, ...]
        """
        season = self.__get_ani_season()
        
        # 镜像列表
        mirrors = [
            'https://openani.an-i.workers.dev',
            'https://pili.cc.cd',
            'https://ani.v300.eu.org',
            'https://ani.op5.de5.net',
        ]
        
        if self._custom_season_url:
            mirrors.insert(0, self._custom_season_url)
        
        for mirror in mirrors:
            self._current_base_url = mirror.rstrip("/")
            url = f'{self._current_base_url}/{season}/'
            logger.info(f'尝试镜像: {url}')
            
            data = self.__make_api_request(url)
            
            if data and 'files' in data:
                result = []
                for item in data['files']:
                    item_info = {
                        'name': item.get('name', ''),
                        'id': item.get('id', ''),
                        'is_folder': item.get('mimeType') == 'application/vnd.google-apps.folder',
                        'created_time': item.get('createdTime', '')
                    }
                    result.append(item_info)
                
                logger.info(f'✅ 成功获取 {len(result)} 个项目（文件/目录），镜像: {mirror}')
                return result
        
        logger.error(f'❌ 所有镜像均无法获取季度 {season} 的数据')
        return []

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_files_from_folder(self, folder_name: str, season: str = None) -> List[Dict]:
        """
        从指定目录名获取文件列表
        使用URL结构：域名/季度/目录名/
        返回格式: [{"name": "文件名", "id": "ID", "download_url": "下载链接"}, ...]
        """
        if not season:
            season = self.__get_ani_season()
        
        base_url = self._current_base_url or 'https://openani.an-i.workers.dev'
        
        # URL编码目录名
        encoded_folder_name = quote(folder_name, safe='')
        url = f'{base_url}/{season}/{encoded_folder_name}/'
        
        logger.info(f'获取目录内容: {folder_name} (URL: {url})')
        
        data = self.__make_api_request(url)
        
        if data and 'files' in data:
            files = []
            for item in data['files']:
                # 只处理媒体文件，忽略子目录
                if item.get('mimeType', '').startswith('video/'):
                    file_info = {
                        'name': item.get('name', ''),
                        'id': item.get('id', ''),
                        'download_url': self._build_download_url(item.get('id', '')),
                        'size': item.get('size', 0),
                        'created_time': item.get('createdTime', '')
                    }
                    files.append(file_info)
            
            logger.info(f'从目录 {folder_name} 获取到 {len(files)} 个媒体文件')
            return files
        
        return []

    def _build_download_url(self, file_id: str) -> str:
        """构建文件下载URL"""
        base_url = self._current_base_url or 'https://openani.an-i.workers.dev'
        
        # 根据API响应构建下载链接
        # 可能的格式：
        # 1. https://域名/file/文件ID?d=true
        # 2. https://域名/季度/目录名/文件名.mp4?d=true
        
        # 暂时使用第一种格式，如果失败再尝试其他格式
        return f'{base_url}/file/{file_id}?d=true'

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        """获取最新更新列表（RSS）"""
        addr = self._custom_rss_url if self._custom_rss_url else 'https://api.ani.rip/ani-download.xml'
        
        try:
            ret = RequestUtils(
                headers=self._get_necessary_headers(),
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
                
                # 域名替换适配
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

    def __touch_strm_file(self, file_name: str, download_url: str, parent_folder: str = "") -> bool:
        """
        创建strm文件
        参数:
            file_name: 文件名
            download_url: 下载链接
            parent_folder: 父目录名（用于创建子文件夹）
        """
        # 构建文件路径
        if parent_folder:
            # 创建番剧子目录
            show_dir = os.path.join(self._storageplace, parent_folder)
            os.makedirs(show_dir, exist_ok=True)
            file_path = os.path.join(show_dir, f'{file_name}.strm')
        else:
            file_path = f'{self._storageplace}/{file_name}.strm'
        
        if os.path.exists(file_path):
            logger.debug(f'{file_name}.strm 文件已存在')
            return False
            
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(download_url)
                logger.debug(f'创建 {file_name}.strm 文件成功')
                return True
        except Exception as e:
            logger.error(f'创建strm源文件失败: {e}')
            return False

    def __task(self, fulladd: bool = False):
        """核心任务执行"""
        cnt = 0
        
        if not fulladd:
            # 增量添加
            rss_info_list = self.get_latest_list()
            logger.info(f'本次处理 {len(rss_info_list)} 个RSS文件')
            for rss_info in rss_info_list:
                if self.__touch_strm_file(file_name=rss_info['title'], download_url=rss_info['link']):
                    cnt += 1
        else:
            # 全量添加当季
            season_items = self.get_current_season_list()
            logger.info(f'本次处理 {len(season_items)} 个季度项目')
            
            for item in season_items:
                if item['is_folder']:
                    # 处理目录：获取目录内文件
                    folder_files = self.get_files_from_folder(item['name'], self.__get_ani_season())
                    for file_info in folder_files:
                        if self.__touch_strm_file(
                            file_name=file_info['name'],
                            download_url=file_info['download_url'],
                            parent_folder=item['name']
                        ):
                            cnt += 1
                else:
                    # 处理文件：直接生成strm
                    download_url = self._build_download_url(item['id'])
                    if self.__touch_strm_file(
                        file_name=item['name'],
                        download_url=download_url
                    ):
                        cnt += 1
                        
        logger.info(f'✅ 新创建了 {cnt} 个strm文件')

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
                    # 基础配置
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
                    # 高级配置
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
                                {'component': 'VTextField', 'props': {'model': 'custom_season', 'label': '自定义季度', 'placeholder': '2024-1'}}
                            ]}
                        ]
                    },
                    # 镜像和RSS配置
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'custom_rss_url', 'label': '自定义RSS订阅源', 'placeholder': '留空使用默认'}}
                            ]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [
                                {'component': 'VTextField', 'props': {'model': 'custom_season_url', 'label': '自定义季度API基础链接', 'placeholder': '留空使用默认'}}
                            ]}
                        ]
                    },
                    # 自定义请求头
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                                {
                                    'component': 'VTextarea',
                                    'props': {
                                        'model': 'custom_headers',
                                        'label': '自定义请求头',
                                        'placeholder': '每行一个，格式: Key: Value\n例如:\nUser-Agent: Mozilla/5.0\nReferer: https://openani.an-i.workers.dev/',
                                        'rows': 6,
                                        'hint': '留空使用内置的浏览器模拟请求头'
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
                                        'type': 'success',
                                        'variant': 'tonal',
                                        'text': '✨ **v3.1.0 新功能**：\n'
                                                '• 支持目录遍历，自动获取番剧文件列表\n'
                                                '• 智能识别文件/目录类型，自动适配处理\n'
                                                '• 为每部番剧创建独立子目录\n'
                                                '• 增强错误处理和日志记录\n\n'
                                                '自动从API抓取下载直链生成strm文件，配合目录监控使用',
                                        'style': 'white-space: pre-line;'
                                    }
                                },
                                {
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'warning',
                                        'variant': 'tonal',
                                        'text': '⚠️ **注意事项**：\n'
                                                '• 确保MoviePilot容器已配置代理\n'
                                                '• 全量添加会遍历目录，请求次数较多\n'
                                                '• 如遇1101错误，请检查代理和请求头配置\n'
                                                '• 建议使用增量更新（RSS）模式更稳定',
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
            "custom_headers": ""
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
            "custom_headers": self._custom_headers
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
