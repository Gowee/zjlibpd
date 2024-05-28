# source: https://github.com/Gowee/wzlibpd/blob/main/uploader/_gen.py
import json
from pathlib import Path
import logging
import os
import csv
import itertools
import datetime
import zhconv_rs as zhconv
from mergedeep import merge, Strategy
import re
import itertools

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)


def zhhant(text):
    if type(text) == str:
        return zhconv.zhconv(text, "zh-Hant")
    else:
        return text


def construct_res_url(resid):
    return "{{ZJLib res link|%s}}" % resid


def construct_pdf_url(blob_id_or_reader):
    if isinstance(blob_id_or_reader, str):
        blob_id = blob_id_or_reader
        return "https://history.zjlib.cn/yz/reader/blobPdf?objectid=" + blob_id
    else:
        reader = blob_id_or_reader
        dir_url = reader["imgUrl"]
        if dir_url.startswith("encodeURIComponent"):
            dir_url = dir_url.removeprefix('encodeURIComponent("')
            dir_url = dir_url.removesuffix('")')
        assert dir_url.endswith("pdfImgaes/"), dir_url  # not our typo, it is imgaes!
        dir_url = dir_url.removesuffix("pdfImgaes/")
        dir_url += (
            reader["readerObj"]["fileName"] + reader["readerObj"]["fileType"]
        )  # e.g. 0001.pdf
        assert dir_url.startswith("http")
        if "54290817-8134-4455-bbed-a02185cfa587" in dir_url:
            dir_url = dir_url.replace(" (2)", "")  # for no reason
        return dir_url


def sanitize_title(s):
    s = s.strip()
    if (
        s
        == "玉海二百卷 辭學指南四卷 詩考一卷 詩地理考六卷 漢藝文志考證十卷 通鑑地理通釋十四卷 漢制考四卷 踐阼篇集解一卷 周易鄭康成注一卷 姓氏急就篇二卷 急就篇補注四卷 周書王會補注一卷 小學紺珠十卷 六經天文篇二卷 通鑑答問五卷"
    ):
        return "玉海二百卷辭學指南四卷詩考一卷詩地理考六卷漢藝文志考證十卷通鑑地理通釋十四卷漢制考四卷踐阼篇集解一卷周易鄭康成注一卷等"
    elif (
        s
        == "新編事文類聚翰墨全書甲集十二卷乙集九卷丙集五卷丁集五卷戊集五卷己集七卷庚集二十四卷辛集十卷壬集十二卷癸集十一卷後甲集八卷後乙集三卷後丙集六卷後丁集八卷後戊集九卷"
    ):
        return "新編事文類聚翰墨全書"
    elif (
        s
        == "兵部尚書恭敏薛公傳一卷；明兵部尚書贈太子太保諡恭敏青雷薛公傳一卷；故兵部尚書贈太子太保諡恭敏伯兄青雷公行狀一卷；明兵部尚書贈太子太保諡恭敏青雷薛公墓誌銘一卷；文武官員祭恭敏公奠稿一卷"
    ):
        return "兵部尚書恭敏薛公傳一卷等"
    s = (
        s.strip()
        .replace("[", "")
        .replace("]", "")
        .replace("（ ", "（")
        .replace(" ）", "）")
        .replace("○", "〇")
    )
    s = re.sub(r"\s+", " ", s)
    return s


def categorize(title, recursive=True):
    if "唐荊川先生纂輯武前編六卷武後編六卷" in title:
        return ["唐荊川先生纂輯武編"]
    if "唐荊川先生編纂左氏始末" in title:
        return ["唐荊川先生編纂左氏始末"]
    if "甲乙集" in title:
        return ["甲乙集"]
    cats = []

    basename = title
    basename = re.sub(
        r"\s*（存[一二三四五六七八九十廿卅卌百千萬佰仟壹貳叄叄肆伍陸柒捌玖拾]+種）\s*",
        "",
        basename,
    )
    basename = re.sub(
        r"存[一二三四五六七八九十廿卅卌百千萬佰仟壹貳叄叄肆伍陸柒捌玖拾]+種\s*：?\s*",
        "",
        basename,
    )
    if m := re.search("【(.+)】", basename):
        cats.append(m.group(1))
        basename = re.sub("【.+】", "", basename)
    if recursive and (m := re.search("（(.+)）$", basename)):
        cats.extend(
            cat
            for cat in categorize(m.group(1), False)
            if len(cat) >= 2
            and cat[-1]
            not in "零〇一二三四五六七八九十廿卅卌百千萬佰仟壹貳叄叄肆伍陸柒捌玖拾至月年日止"
        )
    if re.search(r"^[相鄉饗][著佬]會", basename) and len(basename) > 5:
        basename = re.sub(r"^[相鄉][著佬]會(\d+|-|\s*)", "", basename)
    basename = basename.replace("（匪）", f"\ueeee匪\ueeee")
    basename = re.sub(r"\s*（.+）.*", "", basename)
    basename = basename.replace(f"\ueeee匪\ueeee", "（匪）")
    basename = re.sub(r"復件", "", basename)
    basename = re.sub(r"^(善|[A-Za-z]+)?[-0-9]{2,}\s*", "", basename)
    basename = re.sub(r"^(一名|又名)", "", basename)
    basename = re.sub(r"\s*\(.+\).*", "", basename)

    basename = re.sub(r"-.*$", "", basename)

    basename = basename.replace("中期", "\ueeee中\ueeee期\ueeee")
    basename = basename.replace("中學", "\ueeef中\ueeef學\ueeef")
    basename = basename.replace("小學", "\ueef0小\ueef0學\ueef0")
    basename = basename.replace("始末", "\ueef1始\ueef1末\ueef1")
    basename = basename.replace("本末", "\ueef2始\ueef2末\ueef2")
    basename_ = re.sub(
        r"[-：.·]?\s*([第新]?([零〇一二三四五六七八九十廿卅卌百千佰仟壹兩貳叄叄肆伍陸柒捌玖拾□囗上中下前後首末至甲乙丙丁\-.、，；]+|[0-9至\-.、，；]+)|不分)[期冊卷捲集輯號回篇出種].*$",
        "",
        basename,
    )
    m = re.search(
        r"[-：.·]?\s*(?P<V>[期冊卷捲集輯號回篇出])之?([零〇一二三四五六七八九十廿卅卌百千佰仟壹兩貳叄叄肆伍陸柒捌玖拾□囗上中下前後首末至\-.、，；]+|[0-9至\-.、，；]+)(?P<T>.*)$",
        basename,
    )
    if (
        m
        and (len(basename) - len(m.group(0))) < len(basename_)
        and not any(
            c in "期冊卷捲集輯號回篇出" for c in m.group("T") if c != m.group("V")
        )
    ):
        basename = basename[: (len(basename) - len(m.group(0)))]
    else:
        basename = basename_
    # if len(basename2) < len(basename1):
    #     try:
    #         # e.g. 集一百卷
    #         if any(c in  in  "期冊卷捲集輯號回篇出" for c in basename[len(basename2):] and basename[len(basename2)+1] in "零〇一二三四五六七八九十廿卅卌百千萬佰仟壹兩貳叄叄肆伍陸柒捌玖拾□上中下前後首末至":
    #             basename = basename1
    #         else:
    #             basename = basename2
    #     except IndexError:
    #         basename = basename2
    # else:
    #     basename = basename1

    basename = basename.replace("\ueeee中\ueeee期\ueeee", "中期")
    basename = basename.replace("\ueeef中\ueeef學\ueeef", "中學")
    basename = basename.replace("\ueef0小\ueef0學\ueef0", "小學")
    basename = basename.replace("\ueef1始\ueef1末\ueef1", "始末")
    basename = basename.replace("\ueef2始\ueef2末\ueef2", "本末")
    basename = re.sub(
        r"\s+民國([一二三四五六七八九十廿卅卌百千萬佰仟壹貳叄叄肆伍陸柒捌玖拾至]+|[0-9至]+)年.*$",
        "",
        basename,
    )
    basename = re.sub(
        r"\s+([一二三四五六七八九十廿卅卌百千萬佰仟壹貳叄叄肆伍陸柒捌玖拾至月年日]+|[0-9至年月日]+)度?.*$",
        "",
        basename,
    )
    basename = re.sub(r"\s*(待續|\s[^\s月周期]+[刊號])\s*$", "", basename)
    basename = re.sub(r"(\s+|-)[上中下前後]$", "", basename)
    basename = re.sub(r"\s*[-_0-9]+$", "", basename)
    basename = re.sub(
        r"(\s*|-)[一二三四五六七八九十廿卅卌壹貳叄叄肆伍陸柒捌玖拾至]+$", "", basename
    )  # aggresive
    basename = re.sub(r"\s*(待續|\s[^\s月周期]+[刊號])\s*$", "", basename)
    basename = basename.replace("日札", "日劄")
    basename = re.sub(r"：$", "", basename)

    basename = re.sub(r"^孫[0-9.]+", "", basename)
    basename = basename.replace("1946v2", "")

    if re.search(r"^(日記)?民國", basename) and (
        basename[-1]
        in "零〇一二三四五六七八九十廿卅卌百千萬佰仟壹貳叄叄肆伍陸柒捌玖拾至月年日止"
        or basename[-2:]
        in "甲子|乙丑|丙寅|丁卯|戊辰|己巳|庚午|辛未|壬申|癸酉|甲戌|乙亥|丙子|丁丑|戊寅|己卯|庚辰|辛巳|壬午|癸未|甲申|乙酉|丙戌|丁亥|戊子|己丑|庚寅|辛卯|壬辰|癸巳|甲午|乙未|丙申|丁酉|戊戌|己亥|庚子|辛丑|壬寅|癸卯|甲辰|乙巳|丙午|丁未|戊申|己酉|庚戌|辛亥|壬子|癸丑|甲寅|乙卯|丙辰|丁巳|戊午|[己已]未|庚申|辛酉|壬戌|癸亥"
    ):
        basename = ""
    if re.search("^光緒.+年$", basename):
        basename = ""

    # basename = re.sub(r"(甲子|乙丑|丙寅|丁卯|戊辰|己巳|庚午|辛未|壬申|癸酉|甲戌|乙亥|丙子|丁丑|戊寅|己卯|庚辰|辛巳|壬午|癸未|甲申|乙酉|丙戌|丁亥|戊子|己丑|庚寅|辛卯|壬辰|癸巳|甲午|乙未|丙申|丁酉|戊戌|己亥|庚子|辛丑|壬寅|癸卯|甲辰|乙巳|丙午|丁未|戊申|己酉|庚戌|辛亥|壬子|癸丑|甲寅|乙卯|丙辰|丁巳|戊午|[己已]未|庚申|辛酉|壬戌|癸亥).*$", "", basename)
    basename = basename.removesuffix("·")

    if basename:
        cats.append(basename)
    if not cats:
        logger.info("No cat for " + title)
    if any(re.search(r"\s", cat) for cat in cats):
        logger.info(f"Whitespace in cats {title} -> " + ", ".join(cats))
    if any(re.search(r"卷$", cat) for cat in cats):
        logger.info(f"Trailing 卷 in cats: {title} -> " + ", ".join(cats))
    cats = [
        cat
        for cat in cats
        if not re.search(
            "^([續外別初][集篇編]?|詩鈔|日記|測試|散冊|經文|文集|目錄|甲子|乙丑|丙寅|丁卯|戊辰|己巳|庚午|辛未|壬申|癸酉|甲戌|乙亥|丙子|丁丑|戊寅|己卯|庚辰|辛巳|壬午|癸未|甲申|乙酉|丙戌|丁亥|戊子|己丑|庚寅|辛卯|壬辰|癸巳|甲午|乙未|丙申|丁酉|戊戌|己亥|庚子|辛丑|壬寅|癸卯|甲辰|乙巳|丙午|丁未|戊申|己酉|庚戌|辛亥|壬子|癸丑|甲寅|乙卯|丙辰|丁巳|戊午|[己已]未|庚申|辛酉|壬戌|癸亥)$",
            cat,
        )
    ]
    # assert not any(cat.endswith("·") for cat in cats), cats
    return cats


def generate_category_wikitext(cat):
    return (
        """\
{{Wikidata Infobox}}
{{Category for book|zh}}
{{zh|%s}}

[[Category:Chinese-language books by title]]
"""
        % cat
    )


def gen_toc(reader, indent=0):
    return _gen_toc(reader["readerObj"]["chapters"], indent)


def _gen_toc(section, indent=0):
    indentation = "  " * indent
    if isinstance(section, list):
        return (
            indentation
            + "<ul>\n"
            + "\n".join(_gen_toc(sub, indent=indent + 1) for sub in section)
            + "\n"
            + indentation
            + "</ul>"
        )
    else:
        link = f"{{{{PDF page link|page={section['page']}|text={section['title']}}}}}"
        if subs := section.get("subChapters"):
            toc = indentation + f"<li>{link}"
            toc += _gen_toc(subs, indent).lstrip() + "</li>"
        else:
            toc = "  " * indent + f"<li>{link}</li>"
        return toc


def extract_blob_id(reader):
    dir_url = reader["imgUrl"]
    return re.search(r"/([a-zA-Z0-9-]+)/pdfImgaes", dir_url).group(1)


def gen_attr_fields(attrs, suffix="attr-"):
    # for k,v in attrs.items():
    #     if isinstance(v, list):
    #         assert "獲取方式" == k, f"{k} {v}"
    #     elif isinstance(v, (dict, bool)):
    #         assert False, f"{k} {v}"
    return {
        f"{suffix}{k}": v
        for k, v in attrs.items()
        if isinstance(v, (str, int, float, bool, type(None)))
    }


# def stp(val):
#     """Safe template param"""
#     if val is None:
#         return None
#     return val.replace("[[", "[-{}-[")
