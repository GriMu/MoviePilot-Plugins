import os
import time
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
    """
    :param ExceptionToCheck: 需要捕获的异常
    :param tries: 重试次数
    :param delay: 延迟时间
    :param backoff: 延迟倍数
    :param logger: 日志对象
    :param ret: 默认返回
    """

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"未获取到文件信息，{mdelay}秒后重试 ..."
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


class ANiStrm(_PluginBase):
    # 插件名称
    plugin_name = "OpenANi"
    # 插件描述
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    # 插件版本
    plugin_version = "2.4.3"  # 版本号升级
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
    # 任务执行间隔
    _cron = None
    _onlyonce = False
    _fulladd = False
    _storageplace = None
    
    # ================= 新增自定义配置属性 =================
    _custom_season = None      # 自定义季度，例如 "2024-1"
    _custom_rss_url = None     # 自定义RSS源
    _custom_season_url = None  # 自定义季度API基础链接
    # =====================================================

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        # 停止现有任务
        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._fulladd = config.get("fulladd")
            self._storageplace = config.get("storageplace")
            
            # ================= 加载新增配置 =================
            self._custom_season = config.get("custom_season")
            self._custom_rss_url = config.get("custom_rss_url")
            self._custom_season_url = config.get("custom_season_url")
            # =====================================================
            
        if self._enabled or self._onlyonce:
            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(func=self.__task,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="ANiStrm文件创建")
                    logger.info(f'ANi-Strm定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")

            if self._onlyonce:
                logger.info(f"ANi-Strm服务启动，立即运行一次")
                self._scheduler.add_job(func=self.__task, args=[self._fulladd], trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="ANiStrm文件创建")
                # 关闭一次性开关 全量转移
                self._onlyonce = False
                self._fulladd = False
            self.__update_config()

            # 启动任务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        # 优先使用用户自定义的季度
        if self._custom_season:
            return self._custom_season
            
        current_date = datetime.now()
        current_year = current_date.year
        current_month = idx_month if idx_month else current_date.month
        for month in range(current_month, 0, -1):
            if month in [10, 7, 4, 1]:
                self._date = f'{current_year}-{month}'
                return f'{current_year}-{month}'
        return f'{current_year}-1' # 兜底

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_current_season_list(self) -> List:
        # 优先使用自定义链接，否则使用原默认链接
        base_url = self._custom_season_url if self._custom_season_url else 'https://openani.an-i.workers.dev'
        
        # 拼接URL，确保末尾有斜杠
        url = f'{base_url.rstrip("/")}/{self.__get_ani_season()}/'
        logger.info(f'全量获取季度番剧列表，URL: {url}')

        rep = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                           proxies=settings.PROXY if settings.PROXY else None).post(url=url)
        logger.debug(rep.text)
        files_json = rep.json()['files']
        return [file['name'] for file in files_json]

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        # 优先使用自定义RSS链接，否则使用原默认链接
        addr = self._custom_rss_url if self._custom_rss_url else 'https://api.ani.rip/ani-download.xml'
        logger.info(f'增量获取RSS更新列表，URL: {addr}')
        
        ret = RequestUtils(ua=settings.USER_AGENT if settings.USER_AGENT else None,
                           proxies=settings.PROXY if settings.PROXY else None).get_res(addr)
        ret_xml = ret.text
        ret_array = []
        # 解析XML
        dom_tree = xml.dom.minidom.parseString(ret_xml)
        rootNode = dom_tree.documentElement
        items = rootNode.getElementsByTagName("item")
        for item in items:
            rss_info = {}
            # 标题
            title = DomUtils.tag_value(item, "title", default="")
            # 链接
            link = DomUtils.tag_value(item, "link", default="")
            
            # 如果用户自定义了域名，进行替换适配
            if self._custom_season_url and link:
                # 简单的域名替换逻辑：提取原链接中的相对路径，拼接到自定义域名上
                if "resources.ani.rip" in link:
                    link = link.replace("resources.ani.rip", "openani.an-i.workers.dev")
                # 如果需要更复杂的替换，可以在此处扩展

            rss_info['title'] = title
            rss_info['link'] = link
            ret_array.append(rss_info)
        return ret_array

    def __touch_strm_file(self, file_name, file_url: str = None) -> bool:
        if not file_url:
            # 季度API生成的URL，使用新格式
            base_url = self._custom_season_url if self._custom_season_url else 'https://openani.an-i.workers.dev'
            encoded_filename = quote(file_name, safe='')
            src_url = f'{base_url.rstrip("/")}/{self._date}/{encoded_filename}.mp4?d=true'
        else:
            # 检查API获取的URL格式是否符合要求
            if self._is_url_format_valid(file_url):
                # 格式符合要求，直接使用
                src_url = file_url
            else:
                # 格式不符合要求，进行转换
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
            logger.error('创建strm源文件失败：' + str(e))
            return False

    def _is_url_format_valid(self, url: str) -> bool:
        """检查URL格式是否符合要求（.mp4?d=true）"""
        return url.endswith('.mp4?d=true')

    def _convert_url_format(self, url: str) -> str:
        """将URL转换为符合要求的格式"""
        if '?d=mp4' in url:
            # 将 ?d=mp4 替换为 .mp4?d=true
            return url.replace('?d=mp4', '.mp4?d=true')
        elif url.endswith('.mp4'):
            # 如果已经以.mp4结尾，添加?d=true
            return f'{url}?d=true'
        else:
            # 其他情况，添加.mp4?d=true
            return f'{url}.mp4?d=true'

    def __task(self, fulladd: bool = False):
        cnt = 0
        # 增量添加更新
        if not fulladd:
            rss_info_list = self.get_latest_list()
            logger.info(f'本次处理 {len(rss_info_list)} 个文件')
            for rss_info in rss_info_list:
                if self.__touch_strm_file(file_name=rss_info['title'], file_url=rss_info['link']):
                    cnt += 1
        # 全量添加当季
        else:
            name_list = self.get_current_season_list()
            logger.info(f'本次处理 {len(name_list)} 个文件')
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
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enabled', 'label': '启用插件'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'onlyonce', 'label': '立即运行一次'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'fulladd', 'label': '下次创建当前季度所有番剧strm'}
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '0 0 ? ? ?'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'storageplace',
                                            'label': 'Strm存储地址',
                                            'placeholder': '/downloads/strm'
                                        }
                                    }
                                ]
                            },
                            # ================= 新增UI：自定义季度 =================
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'custom_season',
                                            'label': '自定义季度 (可选)',
                                            'placeholder': '如: 2024-1 (留空自动获取)'
                                        }
                                    }
                                ]
                            }
                            # =====================================================
                        ]
                    },
                    # ================= 新增UI：自定义链接 =================
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'custom_rss_url',
                                            'label': '自定义RSS订阅源 (可选)',
                                            'placeholder': '留空使用默认 ani.rip RSS源'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'custom_season_url',
                                            'label': '自定义季度API基础链接 (可选)',
                                            'placeholder': '留空使用默认 openani Workers链接'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # =====================================================
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '自动从API抓取下载直链生成strm文件，免去人工订阅下载\n'
                                                    '配合目录监控使用，strm文件创建在存储地址，mp会完成刮削\n'
                                                    '现支持自定义季度、RSS源及季度API链接，留空则使用官方默认源。',
                                            'style': 'white-space: pre-line;'
                                        }
                                    },
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'text': '注意: 如果使用自定义季度API链接，全量添加时生成的strm直链也会基于此链接拼接！\n'
                                                    'emby容器需要设置代理环境变量 http_proxy，具体见readme.',
                                            'style': 'white-space: pre-line;'
                                        }
                                    }
                                ]
                            }
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
            # ================= 新增默认值 =================
            "custom_season": "",
            "custom_rss_url": "",
            "custom_season_url": ""
            # =============================================
        }

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "storageplace": self._storageplace,
            # ================= 持久化新增配置 =================
            "custom_season": self._custom_season,
            "custom_rss_url": self._custom_rss_url,
            "custom_season_url": self._custom_season_url
            # =====================================================
        })

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))


if __name__ == "__main__":
    anistrm = ANiStrm()
    name_list = anistrm.get_latest_list()
    print(name_list)
