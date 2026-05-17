import os
import time
import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote, urlparse
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

def retry(ExceptionToCheck: Any, tries: int = 3, delay: int = 3, backoff: int = 1, logger: Any = None, ret: Any = None):
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
    plugin_version = "2.5.3"
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

    def _get_necessary_headers(self) -> dict:
        """获取必要的请求头，自动适配当前镜像"""
        base_url = self._current_base_url or 'https://openani.an-i.workers.dev'
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
                    self._scheduler.add_job(func=self.__task, trigger=CronTrigger.from_crontab(self._cron), name="OpenANi文件创建")
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
        """ 统一的API请求方法 """
        if not headers:
            headers = self._get_necessary_headers()
        headers['Referer'] = url
        try:
            response = RequestUtils(
                headers=headers,
                proxies=settings.PROXY if settings.PROXY else None,
                timeout=30
            ).post(url=url, data=post_data)
            
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as json_err:
                    logger.error(f'JSON解析失败: {json_err}')
                    return None
            else:
                logger.warning(f'请求 {url} 返回状态码: {response.status_code}')
                return None
        except Exception as e:
            logger.error(f'API请求失败: {e}')
            return None

    def _build_standard_download_url(self, original_url: str = "", season: str = None, folder_name: str = None, file_name: str = None) -> str:
        """
        统一构建标准格式的下载链接。
        确保最终链接格式为: https://域名/季度/路径.mp4?d=true
        """
        base_url = self._current_base_url or 'https://openani.an-i.workers.dev'
        if not season:
            season = self.__get_ani_season()

        # 情况1: 来自RSS的链接，包含 ?d=mp4 或特定域名
        if '?d=mp4' in original_url or 'resources.ani.rip' in original_url or 'pro.pili.cc.cd' in original_url:
            # 提取新域名（去掉协议部分）
            if self._custom_season_url:
                new_domain = self._custom_season_url.split('://')[-1].rstrip("/")
            else:
                new_domain = base_url.split('://')[-1]
            # 正则替换：匹配协议和域名，以及可选的 /resources.ani.rip
            pattern = r'(https?://)[^/]+(?:/resources\.ani\.rip)?'
            new_url = re.sub(pattern, f'\\1{new_domain}', original_url)
            
            if '?d=mp4' in new_url:
                new_url = new_url.replace('?d=mp4', '.mp4?d=true')
            elif new_url.endswith('.mp4'):
                new_url = f"{new_url}?d=true"
            else:
                new_url = f"{new_url}.mp4?d=true"
            return new_url

        # 情况2: 明确提供了文件名（来自API全量遍历，最稳定可靠的方式）
        elif file_name:
            if folder_name:
                encoded_folder = quote(folder_name, safe='')
                encoded_file = quote(file_name, safe='')
                new_url = f"{base_url}/{season}/{encoded_folder}/{encoded_file}"
            else:
                encoded_file = quote(file_name, safe='')
                new_url = f"{base_url}/{season}/{encoded_file}"
            
            if not new_url.endswith('.mp4?d=true'):
                if new_url.endswith('.mp4'):
                    new_url = f"{new_url}?d=true"
                else:
                    new_url = f"{new_url}.mp4?d=true"
            return new_url

        # 情况3: 兜底处理（理论上不应该走到这里）
        logger.warning(f"无法识别的链接构建参数，原样返回: {original_url}")
        return original_url

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_current_season_list(self) -> List[Dict]:
        """ 获取当前季度番剧列表 """
        season = self.__get_ani_season()
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
                logger.info(f'✅ 成功获取 {len(result)} 个项目，镜像: {mirror}')
                return result
        logger.error(f'❌ 所有镜像均无法获取季度 {season} 的数据')
        return []

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_files_from_folder(self, folder_name: str, season: str = None) -> List[Dict]:
        """ 从指定目录名获取文件列表，并直接构建标准直链 """
        if not season:
            season = self.__get_ani_season()
        base_url = self._current_base_url or 'https://openani.an-i.workers.dev'
        
        encoded_folder_name = quote(folder_name, safe='')
        url = f'{base_url}/{season}/{encoded_folder_name}/'
        logger.info(f'获取目录内容: {folder_name} (URL: {url})')
        
        data = self.__make_api_request(url)
        if data and 'files' in data:
            files = []
            for item in data['files']:
                if item.get('mimeType', '').startswith('video/'):
                    file_info = {
                        'name': item.get('name', ''),
                        'id': item.get('id', ''),
                        # 关键修改：直接使用文件名构建直链，摒弃ID拼接方式
                        'download_url': self._build_standard_download_url(
                            season=season,
                            folder_name=folder_name,
                            file_name=item.get('name', '')
                        ),
                        'size': item.get('size', 0),
                        'created_time': item.get('createdTime', '')
                    }
                    files.append(file_info)
            logger.info(f'从目录 {folder_name} 获取到 {len(files)} 个媒体文件')
            return files
        return []

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        """获取最新更新列表（RSS），直接在此处转换为标准直链"""
        addr = self._custom_rss_url if self._custom_rss_url else 'https://anirss.581618.xyz/ani-download.xml'
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
            dom_tree = xml.dom.minidom.parseString(ret_xml)
            rootNode = dom_tree.documentElement
            items = rootNode.getElementsByTagName("item")
            
            for item in items:
                title = DomUtils.tag_value(item, "title", default="")
                link = DomUtils.tag_value(item, "link", default="")
                
                if link and title:
                    # 关键修改：获取到RSS链接后立即通过统一函数转换
                    standard_link = self._build_standard_download_url(original_url=link)
                    ret_array.append({
                        'title': title,
                        'link': standard_link
                    })
            return ret_array
        except Exception as e:
            logger.error(f'获取RSS列表失败: {e}')
            return []

    def __touch_strm_file(self, file_name: str, download_url: str, parent_folder: str = "") -> bool:
        """ 创建strm文件 """
        if parent_folder:
            show_dir = os.path.join(self._storageplace, parent_folder)
            os.makedirs(show_dir, exist_ok=True)
            file_path = os.path.join(show_dir, f'{file_name}.strm')
            log_prefix = f'[{parent_folder}] '
        else:
            file_path = f'{self._storageplace}/{file_name}.strm'
            log_prefix = ''

        if os.path.exists(file_path):
            logger.debug(f'{log_prefix}{file_name}.strm 文件已存在，跳过')
            return False

        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(download_url)
            logger.info(f'🏷 {log_prefix}创建 {file_name}.strm -> {download_url}')
            return True
        except Exception as e:
            logger.error(f'创建 {file_name}.strm 失败: {e}')
            return False

    def _extract_season_from_url(self, url: str) -> str:
        """从URL中提取季度路径，如 2026-4"""
        match = re.search(r'/(\d{4}-[1-9])/', url)
        return match.group(1) if match else ""

    def __task(self, fulladd: bool = False):
        """核心任务执行"""
        cnt = 0
        if not fulladd:
            # 增量添加 (此时 download_url 已经在 get_latest_list 中被标准化)
            rss_info_list = self.get_latest_list()
            logger.info(f'本次处理 {len(rss_info_list)} 个RSS文件')
            for rss_info in rss_info_list:
                season_folder = self._extract_season_from_url(rss_info['link'])
                if self.__touch_strm_file(file_name=rss_info['title'], download_url=rss_info['link'], parent_folder=season_folder):
                    cnt += 1
        else:
            # 全量添加当季
            season_items = self.get_current_season_list()
            logger.info(f'本次处理 {len(season_items)} 个季度项目')
            for item in season_items:
                if item['is_folder']:
                    # 处理目录 (此时 download_url 已经在 get_files_from_folder 中被标准化)
                    folder_files = self.get_files_from_folder(item['name'], self.__get_ani_season())
                    for file_info in folder_files:
                        if self.__touch_strm_file(
                            file_name=file_info['name'],
                            download_url=file_info['download_url'],
                            parent_folder=item['name']
                        ):
                            cnt += 1
                else:
                    # 处理季度下的直接文件：使用文件名构建标准直链
                    standard_url = self._build_standard_download_url(
                        season=self.__get_ani_season(),
                        file_name=item['name']
                    )
                    if self.__touch_strm_file(
                        file_name=item['name'],
                        download_url=standard_url
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
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                                {
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'success',
                                        'variant': 'tonal',
                                        'text': '✨ **v3.2.0 更新**：\n'
                                                '• 强制统一直链格式：/季度/路径.mp4?d=true\n'
                                                '• 修复RSS源域名不一致导致的格式错误\n'
                                                '• 优化全量遍历，基于文件名构建直链，彻底弃用文件ID拼接\n'
                                                '• 自动从API抓取下载直链生成strm文件，配合目录监控使用',
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
                                                '• 如遇请求错误，请检查代理和请求头配置\n'
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
