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
    return f"https://history.zjlib.cn/app/universal-search/resource/{resid}/details?wfwfid=2120&searchId=24016&params=&pageId=107556&classifyId=&classifyName="


def construct_pdf_url(blob_id):
    return "https://history.zjlib.cn/yz/reader/blobPdf?objectid=" + blob_id


def sanitize_title(s):
    if (
        s
        == "皇清誥授中憲大夫湖北糧儲道祖考敏齋公行狀一卷（ 林培厚行狀） 誥授中憲大夫湖北督糧道林公墓誌銘一卷（林培厚墓誌銘） 敕封文林郎林君墓誌銘一卷（林培厚墓誌銘）"
    ):
        return "皇清誥授中憲大夫湖北糧儲道祖考敏齋公行狀一卷 誥授中憲大夫湖北督糧道林公墓誌銘一卷 敕封文林郎林君墓誌銘一卷"
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
    if "過來語" in title:
        return ["過來語"]
    if "利濟學堂" in title and "匯" in title:
        return ["利濟學堂扱匯"]
    if "瑞安縣誌稿" in title:
        return ["瑞安縣誌稿"]
    if "蛻庵日札" in title:
        return ["蛻庵日札"]
    if "uian縣立簡易師範學校同學錄" in title:
        return ["瑞安縣立簡易師範學校同學錄"]
    if "符笑拈日記" in title:
        return ["符笑拈日記"]
    if "范氏奇書" in title:
        return ["范氏奇書"]
    if "浙江省均賦問題" in title:
        return ["浙江省均賦問題"]
    if "整理土地三計劃" in title:
        return ["整理土地三計劃"]
    if "【麗岙街道葉宅村】潁川郡陳氏宗譜 麗岙" in title:
        return ["麗岙街道葉宅村", "潁川郡陳氏宗譜"]
    if re.search(r"^蠡.日.", title):
        return ["蠡傭日劄"]
    if re.search("^暖姝室日.", title):
        return ["暖姝室日劄"]
    if re.search("^翳彗.[齋斋]", title):
        return ["翳彗旍齋日劄"]
    if re.search("^種瓜[廬盧]日.", title):
        return ["種瓜廬日劄"]
    if "萬萬庵" in title:
        return ["萬萬庵"]
    if "蛻庵日札" in title:
        return ["蛻庵日劄"]
    if "知昨非斎日" in title:
        return ["知昨非斎日劄"]
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
    basename_ = re.sub(
        r"[-：.·]?\s*([第新]?([零〇一二三四五六七八九十廿卅卌百千萬佰仟壹兩貳叄叄肆伍陸柒捌玖拾□上中下前後首末至\-.、，；]+|[0-9至\-.、，；]+)|不分)[期冊卷捲集輯號回篇出].*$",
        "",
        basename,
    )
    m = re.search(
        r"[-：.·]?\s*(?P<V>[期冊卷捲集輯號回篇出])之?([零〇一二三四五六七八九十廿卅卌百千萬佰仟壹兩貳叄叄肆伍陸柒捌玖拾□上中下前後首末至\-.、，；]+|[0-9至\-.、，；]+)(?P<T>.*)$",
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
    if "街道" in cat or "澤雅鎮周岙" in cat:
        return (
            """{{Wikidata Infobox}}
{{zh|%s}}

[[Category:County-level divisions of Wenzhou]]
"""
            % cat
        )
    else:
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
            toc = indentation + f"<li>{link}\n"
            toc += _gen_toc(subs, indent) + "</li>\n"
        else:
            toc = "  " * indent + f"<li>{link}</li>"
        return toc


def extract_blob_id(reader):
    dir_url = reader["imgUrl"]
    return re.search(r"/([a-zA-Z0-9-]+)/pdfImgaes", dir_url).group(1)
