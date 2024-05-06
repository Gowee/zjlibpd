#!/usr/bin/env python3
#
from _gen import *
import builtins

DATA_PATH = Path(__file__).parent / "../crawler/items.json.all"


def main():
    mapping = {}
    books = []
    with open(DATA_PATH) as f:
        for line in f:
            line = json.loads(line)
            mapping[line["id"]] = line

    # redundant validation
    tlitems = set(mapping.keys())  # top-level items
    for _id, item in mapping.items():
        for sub in item.get("sub_resources", []):
            sub["__ATTRS__"] = {
                zhhant(field["key"]): field.get("subs") or zhhant(field["value"])
                for field in sub["fields"]
            }
            tlitems.discard(sub["id"])
        item["__ATTRS__"] = {
            zhhant(field["key"]): field.get("subs") or zhhant(field["value"])
            for field in item["fields"]
        }
    for id in tlitems:
        # tagged by our crawler
        assert mapping[id].get("__PARENT__") is None, id
        # commented out because we manually added several broken ones
        # assert mapping[id].get('__MERGED__') is True

    uploads = []
    indices = [[]]
    counts = [0]
    built_categories = set()

    for id in tlitems:
        item = mapping[id]
        if len(indices[-1]) >= 10000:
            indices.append([])
            counts.append(0)

        title = item["__ATTRS__"]["題名"]
        sanitized_title = sanitize_title(title)
        categories = set(categorize(sanitized_title))

        if reader := item.get("__READER__"):
            assert item.get("sub_resources") is None
            vols = [
                (
                    extract_blob_id(sub)
                    if isinstance(sub, dict)
                    else re.search(r"objectid=([\w-]+)($|&)", reader[0]).group(1),
                    sub,
                )
                for ii, sub in enumerate(reader)
            ]
            if len(vols) == 1:
                # (re.search(r"objectid=([\w-]+)($|&)", reader[0]).group(1)
                vols = [(f"ZJLib-{item['id']} {title}.pdf", vols[0])]
            else:
                vols = [
                    (f"ZJLib-{item['id']}-{i+1} {title} 第{ii+1}冊.pdf", vol)
                    for ii, vol in enumerate(vols)
                ]
        else:
            subs = item.get("sub_resources")
            if not subs:
                logger.warning(f"No reader and sub_resources for {item['id']}")
                continue
            seen_blobs = set()
            blobs = []
            assert len(subs) > 0, item["id"]
            for i, sub in enumerate(subs):
                sub = merge(
                    sub, mapping.get(sub["id"], {}), strategy=Strategy.TYPESAFE_ADDITIVE
                )
                if sub.get("__READER__"):
                    assert len(sub["__READER__"]) == 1
                    blob_id = extract_blob_id(sub["__READER__"][0])
                else:
                    try:
                        reader_field = next(
                            field
                            for field in sub["fields"]
                            if field["key"] == "获取方式" or field["key"] == "阅读"
                        )
                    except StopIteration:
                        logger.warn(f"No file for {item['id']}->{sub['id']}")
                        continue
                    if not (ors_url := reader_field.get("orsUrl")):
                        assert len(reader_field["subs"]) == 1
                        ors_url = reader_field["subs"][0]["orsUrl"]
                    blob_id = re.search(r"fileId%3D([a-z0-9-]+)%", ors_url).group(1)
                if not blobs or blob_id not in seen_blobs:
                    blobs.append((blob_id, [sub]))
                else:
                    if blobs[-1][0] != blob_id:
                        logger.warn(
                            f"Volumes are unordered for {item['id']}->{blob_id}@{i} (seen {','.join(blob_id for blob_id, _ in blobs)})"
                        )
                        # assert blobs[-1][0] == blob_id, f"{item['id']} {blob_id}"
                    blobs[-1][1].append(sub)
                seen_blobs.add(blob_id)

            if not blobs:
                logger.warning(
                    f"No files for {item['id']} (subres={len(item['sub_resources'])})"
                )
                continue
            assert blobs, item["id"]
            if len(blobs) == 1:
                filename = f"ZJLib-{item['id']} {title}.pdf"
                vols = [(filename, (blobs[0]))]
            else:
                vols = []
                for i, (blob_id, subs) in enumerate(blobs):
                    filename = f"ZJLib-{item['id']}-{i+1} {title} 第{i+1}冊.pdf"
                    vols.append((filename, (blob_id, subs)))

        if len(vols) == 1:
            indices[-1].append(f"* [[:{vols[0][0]}]]{[construct_res_url(item['id'])]}")
            counts[-1] += 1
        else:
            indices[-1].append(f"* {title}{[construct_res_url(item['id'])]}")

        attr_fields = {f"attr-{k}": v for k, v in item["__ATTRS__"].items()}
        prev_filename, prev_vol = None, None
        for i in range(len(vols)):
            filename, vol = vols[i]
            if i + 1 < len(vols):
                next_filename, next_vol = vols[i + 1]

            fields = {"blobid": blob_id}
            ress = []
            match vol:
                case (blob_id, _url) if isinstance(_url, str):
                    # url
                    fields["resid"] = item["id"]
                    fields["resname"] = item["__ATTRS__"]["題名"]
                    ress.append((item["id"], item["__ATTRS__"]["題名"]))
                    break
                case (blob_id, reader) if isinstance(reader, dict):
                    # reader obj
                    fields["resid"] = item["id"]
                    fields["resname"] = item["__ATTRS__"]["題名"]
                    ress.append((item["id"], item["__ATTRS__"]["題名"]))
                    toc = gen_toc(vol[1])
                    fields["toc"] = toc
                    blob_id = extract_blob_id(vol[1])
                case (blob_id, subs) if isinstance(subs, list):
                    # the blob file spans over multiple resources
                    for ii, sub in enumerate(subs):
                        fields[f"resid{ii+1}"] = sub["id"]
                        fields[f"resname{ii+1}"] = sub["__ATTRS__"]["題名"]
                        ress.append((sub["id"], sub["__ATTRS__"]["題名"]))
                    reader = None
                    for sub in subs:
                        # some reader pages are broken (NULL POINTER), we try to find a valid one
                        if reader := sub.get("__READER__"):
                            assert len(reader) == 1
                            fields["toc"] = gen_toc(reader[0])
                            break
                    if len(subs) == 1:
                        attr_fields |= attr_fields
                case _:
                    raise NotImplementedError
            url = construct_pdf_url(blob_id)
            fields |= attr_fields
            fields["searchid"] = 24016
            fields_wikitext = "\n".join([f"  |{k}={v}" for k, v in fields.items()])
            booknav_wikitext = ""
            if len(vols) > 1:
                indices[-1].append(
                    f"** [[:{filename}]]："
                    + "；".join(f"{name}[{construct_res_url(id)}]" for id, name in ress)
                )
                counts[-1] += 1
                # fmt: off
                booknav_wikitext = f"\n{{{{ZJLibBookNaviBar|prev={prev_filename or ""}|next={next_filename or ""}|parentresid={item['id']}|nth={i+1}|total={len(vols)}}}}}\n"
            wikitext = f"""=={{{{int:filedesc}}}}==\
{booknav_wikitext}\
{{{{Book in the Zhejiang Library
{fields_wikitext}
}}}}

""" + "".join(
                f"[[Category:{cat}]]\n" for cat in categories
            )

            resids_tag = item["id"]
            if len(ress) > 1:
                resids_tag += "->" + ",".join(id for id, name in ress)
            cats_tag = ""
            if categories:
                cats_tag = "; " + ", ".join(
                    f"[[:c:Category:{cat}|{cat}]]" for cat in categories
                )
            uploads.append(
                (
                    "File:" + filename,
                    wikitext,
                    f"{title} (batch task; zjlib:{resids_tag}; blob:{blob_id}; {i+1}/{len(vols)} of {item['__ATTRS__']['題名']}{cats_tag})",
                    url,
                )
            )

            prev_filename, prev_vol = filename, vol
        for cat in categories:
            if cat in built_categories:
                continue
            built_categories.add(cat)
            category_wikitext = generate_category_wikitext(cat)
            uploads.append(
                (
                    "Category:" + cat,
                    category_wikitext,
                    f"{title} -> {cat} (batch task; zjlib:{item['id']})",
                    None,
                )
            )

    #         if subs := item.get('sub_resource'): # TODO: sub in field
    #             # multi-volume book
    #             title = item['__ATTRS__']['題名']
    #             sanitized_title = sanitize_title(title)
    #             base_categories = set(categorize(sanitized_title))

    #             indices[-1].append(
    #                 f"* {title}[https://history.zjlib.cn/app/universal-search/resource/{item['ID']}/details?wfwfid=2120&searchId=24016&params=&pageId=107556&classifyId=&classifyName=]"
    #             )

    #             item['__ATTRS__']['獲取方式']

    #             vols = []
    #             for sub in subs:
    #                 filename = f"ZJLib-{book['ID']} {title}.pdf"
    #                 vols.append((filename, sub))
    #                 pass
    #         else:
    #             # single-volume book
    #             title = item['__ATTRS__']['題名']
    #             sanitized_title = sanitize_title(title)
    #             categories = set(categorize(sanitized_title))

    #             filename = f"ZJLib-{item['id']} {sanitized_title}.pdf"
    #             indices[-1].append(
    #                 f"* [[:File:{filename}]][https://history.zjlib.cn/app/universal-search/resource/{item['ID']}/details?wfwfid=2120&searchId=24016&params=&pageId=107556&classifyId=&classifyName=]"
    #             )
    #             counts[-1] += 1
    #             attr_fields = "\n".join(
    #                 [f"  |attr-{k}={v or ''}" for k, v in (item["__ATTRS__"] or {}).items() if isinstance(v, (str, int, float, None, bool))]
    #             )

    #             wikitext = f"""=={{{{int:filedesc}}}}==
    # {{{{Book in the Zhejiang Library
    #   |blobid
    #   |resid={book['id']}
    #   |resname={title}
    # {attr_fields}
    # }}}}

    # """ + "".join(
    #                 f"[[Category:{cat}]]\n" for cat in categories
    #             )
    #             url = construct_pdf_url()
    #             uploads.append(
    #                 (
    #                     "File:" + filename,
    #                     wikitext,
    #                     f"{title} (batch task; zjlib:{item['id']}"
    #                     + (
    #                         (
    #                             "; "
    #                             + ", ".join(
    #                                 f"[[:c:Category:{cat}|{cat}]]" for cat in categories
    #                             )
    #                         )
    #                         if categories
    #                         else ""
    #                     )
    #                     + ")",
    #                     url,
    #                 )
    #             )

    #             for cat in categories:
    #                 if cat in built_categories:
    #                     continue
    #                 built_categories.add(cat)
    #                 category_wikitext = generate_category_wikitext(cat)
    #                 uploads.append(
    #                     (
    #                         "Category:" + cat,
    #                         category_wikitext,
    #                         f"{title} -> {cat} (batch task; zjlib:{item['id']})",
    #                         None,
    #                     )
    #                 )

    #     subs = set()
    #     for book in books:
    #         if book["DigitalResourceData"]:
    #             bookname = zhhant(book["Title"])
    #             base_categories = set(categorize(sanitize_title(bookname)))

    #             if len(indices[-1]) >= 10000:
    #                 indices.append([])
    #                 counts.append(0)
    #             indices[-1].append(
    #                 f"* {bookname}[https://db.wzlib.cn/detail.html?id={book['ID']}]"
    #             )

    #             vols = []
    #             for sub in book["DigitalResourceData"]:
    #                 subs.add(sub["Url"])
    #                 vol = by_pdf_url[sub["Url"]]
    #                 assert vol["Title"] == sub["Title"], f"{vol} {sub}"
    #                 title = sanitize_title(zhhant(vol["Title"]))
    #                 filename = f"WZLib-DB-{vol['ID']} {title}.pdf"
    #                 vols.append((filename, vol))
    #             prev_filename, last_vol = None, None
    #             # book_attr_fields =
    #             for i in range(len(vols)):
    #                 filename, vol = vols[i]
    #                 next_filename, next_vol = (
    #                     vols[i + 1] if i + 1 < len(vols) else (None, None)
    #                 )

    #                 indices[-1].append(
    #                     f"** [[:File:{filename}]][https://db.wzlib.cn/detail.html?id={vol['ID']}]"
    #                 )
    #                 counts[-1] += 1

    #                 attrs = book["ATTRS"] or {}
    #                 for k, v in (vol["ATTRS"] or {}).items():
    #                     if not attrs.get(k):
    #                         attrs[k] = v
    #                 # assert book['ATTRS'] == vol['ATTRS'], vol['ATTRS']

    #                 attr_fields = "\n".join(
    #                     [f"  |attr-{k}={v or ''}" for k, v in attrs.items()]
    #                 )
    #                 volname = zhhant(vol["Title"])
    #                 categories = sorted(
    #                     base_categories | set(categorize(sanitize_title(volname)))
    #                 )

    #                 wikitext = f"""=={{{{int:filedesc}}}}==
    # {{{{WZLibDBBookNaviBar|prev={prev_filename or ""}|next={next_filename or ""}|nth={i+1}|total={len(vols)}|bookname={bookname}}}}}
    # {{{{Book in the Wenzhou Library DB
    #   |id={vol['ID']}
    #   |title={volname}
    #   |bookid={book['ID']}
    #   |booktitle={bookname}
    # {attr_fields}
    # }}}}

    # """ + "".join(
    #                     f"[[Category:{cat}]]\n" for cat in categories
    #                 )
    #                 url = construct_pdf_url(vol["pdf_url"])
    #                 uploads.append(
    #                     (
    #                         "File:" + filename,
    #                         wikitext,
    #                         f"{vol['Title']} (batch task; wzlibdb:{vol['ID']}; {i + 1}/{len(vols)} of {bookname}"
    #                         + (
    #                             (
    #                                 "; "
    #                                 + ", ".join(
    #                                     f"[[:c:Category:{cat}|{cat}]]" for cat in categories
    #                                 )
    #                             )
    #                             if categories
    #                             else ""
    #                         )
    #                         + ")",
    #                         url,
    #                     )
    #                 )
    #                 prev_filename, prev_vol = vols[i]

    #                 for cat in categories:
    #                     if cat in built_categories:
    #                         continue
    #                     built_categories.add(cat)
    #                     category_wikitext = generate_category_wikitext(cat)
    #                     uploads.append(
    #                         (
    #                             "Category:" + cat,
    #                             category_wikitext,
    #                             f"{book['Title']} -> {cat} (batch task; wzlibdb:{book['ID']})",
    #                             None,
    #                         )
    #                     )

    #     for book in books:
    #         if book["pdf_url"] and book["pdf_url"] not in subs:
    #             if len(indices[-1]) >= 10000:
    #                 indices.append([])
    #                 counts.append(0)
    #             title = sanitize_title(zhhant(book["Title"]))
    #             categories = categorize(title)
    #             filename = f"WZLib-DB-{book['ID']} {title}.pdf"
    #             indices[-1].append(
    #                 f"* [[:File:{filename}]][https://db.wzlib.cn/detail.html?id={book['ID']}]"
    #             )
    #             counts[-1] += 1
    #             attr_fields = "\n".join(
    #                 [f"  |attr-{k}={v or ''}" for k, v in (book["ATTRS"] or {}).items()]
    #             )
    #             bookname = zhhant(book["Title"])

    #             wikitext = f"""=={{{{int:filedesc}}}}==
    # {{{{Book in the Wenzhou Library DB
    #   |id={book['ID']}
    #   |title={bookname}
    # {attr_fields}
    # }}}}

    # """ + "".join(
    #                 f"[[Category:{cat}]]\n" for cat in categories
    #             )
    #             url = construct_pdf_url(book["pdf_url"])
    #             uploads.append(
    #                 (
    #                     "File:" + filename,
    #                     wikitext,
    #                     f"{book['Title']} (batch task; wzlibdb:{book['ID']}"
    #                     + (
    #                         (
    #                             "; "
    #                             + ", ".join(
    #                                 f"[[:c:Category:{cat}|{cat}]]" for cat in categories
    #                             )
    #                         )
    #                         if categories
    #                         else ""
    #                     )
    #                     + ")",
    #                     url,
    #                 )
    #             )

    #             for cat in categories:
    #                 if cat in built_categories:
    #                     continue
    #                 built_categories.add(cat)
    #                 category_wikitext = generate_category_wikitext(cat)
    #                 uploads.append(
    #                     (
    #                         "Category:" + cat,
    #                         category_wikitext,
    #                         f"{book['Title']} -> {cat} (batch task; wzlibdb:{book['ID']})",
    #                         None,
    #                     )
    #                 )

    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    uploads_file_path = f"zjlib-uploads-{timestamp}.tsv"
    with open(
        uploads_file_path,
        "w",
    ) as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerows(uploads)

    indices_file_path = f"zjlib-indices-{timestamp}.tsv"
    with open(
        indices_file_path,
        "w",
    ) as f:
        if len(indices) > 1:
            w = csv.writer(f, delimiter="\t", lineterminator="\n")
            for i in range(len(indices)):
                w.writerow(
                    [
                        f"Commons:Library_back_up_project/file_list/ZJLib/{i+1:02}",
                        "\n".join(indices[i]) + "\n",
                        f"Count: {counts[i]}/{sum(counts)}",
                        None,
                    ]
                )
        else:
            f.write('Commons:Library_back_up_project/file_list/ZJLib\t"')
            f.writelines(map(lambda line: line + "\n", indices[0]))
            f.write(f'"\tCount: {counts[-1]}\t')

    logger.info(f"Written {uploads_file_path}, {indices_file_path}")


if __name__ == "__main__":
    main()
