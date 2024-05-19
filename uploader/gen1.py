#!/usr/bin/env python3
#
from _gen import *
import builtins

DATA_PATH = Path(__file__).parent / "../crawler/items.json.all"


def main():
    mapping = {}
    with open(DATA_PATH) as f:
        for line in f:
            line = json.loads(line)
            mapping[line["id"]] = line

    # redundant validation
    subitems = set()
    for _id, item in mapping.items():
        for sub in item.get("sub_resources", []):
            sub["__ATTRS__"] = {
                zhhant(field["key"]): field.get("subs") or zhhant(field["value"])
                for field in sub["fields"]
            }
            if sub["__ATTRS__"]["類型"] == "卷":
                subitems.add(sub["id"])
        item["__ATTRS__"] = {
            zhhant(field["key"]): field.get("subs") or zhhant(field["value"])
            for field in item["fields"]
        }
    tlitems = [id for id in mapping.keys() if id not in subitems]  # top-level items
    # we rely on the insertion order guarantee of dict for stable ordering
    for id in tlitems:
        # tagged by our crawler
        assert mapping[id].get("__PARENT__") is None, id
        # commented out because we manually added several broken ones
        # assert mapping[id].get('__MERGED__') is True
    logger.info(f"Top-level items: {len(tlitems)}")

    uploads = []
    indices = [[]]
    counts = [0]
    built_categories = set()

    for id in tlitems:
        item = mapping[id]
        if len(indices[-1]) >= 2000:
            indices.append([])
            counts.append(0)

        title = item["__ATTRS__"]["題名"]
        sanitized_title = sanitize_title(title)
        categories = set(categorize(sanitized_title))
        unordered = False

        if reader := item.get("__READER__"):
            assert item.get("sub_resources") is None
            vols = [
                (
                    extract_blob_id(sub)
                    if isinstance(sub, dict)
                    else re.search(r"objectid=([\w-]+)($|&)", reader[0]).group(1),
                    sub,
                )
                for _ii, sub in enumerate(reader)
            ]
            if len(vols) == 1:
                # (re.search(r"objectid=([\w-]+)($|&)", reader[0]).group(1)
                vols = [(f"ZJLib-{item['id']} {sanitized_title}.pdf", vols[0])]
            else:
                vols = [
                    (f"ZJLib-{item['id']}-{i+1} {sanitized_title} 第{ii+1}冊.pdf", vol)
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
            # sometimes, the parent resource is included as a sub resource of itself
            # so we filter them by type
            for i, sub in enumerate(
                filter(lambda sub: sub["__ATTRS__"]["類型"] == "卷", subs)
            ):
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
                        logger.warning(f"No file for {item['id']}->{sub['id']}")
                        continue
                    if not (ors_url := reader_field.get("orsUrl")):
                        assert len(reader_field["subs"]) == 1
                        ors_url = reader_field["subs"][0]["orsUrl"]
                    blob_id = re.search(r"fileId%3D([a-z0-9-]+)%", ors_url).group(1)
                if not blobs or blob_id not in seen_blobs:
                    blobs.append((blob_id, [sub]))
                else:
                    if blobs[-1][0] != blob_id:
                        logger.warning(
                            f"Volumes are unordered for {item['id']}->{blob_id}@{i} (seen {','.join(blob_id for blob_id, _ in blobs)})"
                        )
                        unordered = True
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
                filename = f"ZJLib-{item['id']} {sanitized_title}.pdf"
                vols = [(filename, (blobs[0]))]
            else:
                vols = []
                for i, (blob_id, subs) in enumerate(blobs):
                    filename = (
                        f"ZJLib-{item['id']}-{i+1} {sanitized_title} 第{i+1}冊.pdf"
                    )
                    vols.append((filename, (blob_id, subs)))

        cats_list = "".join(f"[[:Category:{cat}|{cat}]]" for cat in categories)
        if cats_list:
            cats_list = " -> " + cats_list
        if len(vols) == 1:
            indices[-1].append(
                f"* [[:File:{vols[0][0]}]]{construct_res_url(item['id'])}" + cats_list
            )
            counts[-1] += 1
        else:
            indices[-1].append(f"* {title}{construct_res_url(item['id'])}" + cats_list)

        prev_filename, prev_vol = None, None
        for i in range(len(vols)):
            filename, vol = vols[i]
            assert (
                l := len(filename.encode("utf-8"))
            ) < 240, f"Filename too long: {item['id']} {filename} {l} > 240"
            # if (l := len(filename.encode("utf-8"))) > 240:
            #     logger.warning(f"Filename too long: {item['id']} {filename} {l} > 240")
            if i + 1 < len(vols):
                next_filename, next_vol = vols[i + 1]
            fields = {"blobid": vol[0]}
            ress = []
            match vol:
                case (blob_id, _url) if isinstance(_url, str):
                    # url
                    fields["resid"] = item["id"]
                    fields["resname"] = item["__ATTRS__"]["題名"]
                    ress.append((item["id"], item["__ATTRS__"]["題名"]))
                    fields |= gen_attr_fields(item["__ATTRS__"], f"attr-")
                    url = construct_pdf_url(blob_id)
                case (blob_id, reader) if isinstance(reader, dict):
                    # reader obj
                    fields["resid"] = item["id"]
                    fields["resname"] = item["__ATTRS__"]["題名"]
                    ress.append((item["id"], item["__ATTRS__"]["題名"]))
                    fields |= gen_attr_fields(item["__ATTRS__"], f"attr-")
                    toc = gen_toc(vol[1])
                    fields["toc"] = toc
                    # blob_id = extract_blob_id(vol[1])
                    url = construct_pdf_url(reader)
                case (blob_id, subs) if isinstance(subs, list):
                    # the blob file spans over multiple resources
                    fields["nth"] = i + 1
                    fields["total"] = len(vols)
                    for ii, sub in enumerate(
                        filter(lambda sub: sub["__ATTRS__"]["類型"] == "卷", subs)
                    ):
                        fields[f"resid{ii+1}"] = sub["id"]
                        fields[f"resname{ii+1}"] = sub["__ATTRS__"]["題名"]
                        ress.append((sub["id"], sub["__ATTRS__"]["題名"]))
                        fields |= gen_attr_fields(sub["__ATTRS__"], f"attr{ii+1}-")
                    fields["parentresid"] = item["id"]
                    fields["parentresname"] = item["__ATTRS__"]["題名"]
                    fields |= {
                        f"parentattr-{k}": v for k, v in item["__ATTRS__"].items()
                    }
                    reader = None
                    url = construct_pdf_url(blob_id)
                    for sub in subs:
                        # some reader pages are broken (java NULL POINTER), we try to find a valid one
                        if reader := sub.get("__READER__"):
                            assert len(reader) == 1
                            fields["toc"] = gen_toc(reader[0])
                            url = construct_pdf_url(reader[0])
                            break
                    # if len(subs) == 1:
                    #     attr_fields |= attr_fields
                case _:
                    raise NotImplementedError
            fields["searchid"] = 24016
            fields_wikitext = "\n".join(
                [f"  |{k}={'' if v is None else v}" for k, v in fields.items()]
            )
            booknav_wikitext = ""
            if len(vols) > 1:
                indices[-1].append(
                    f"** [[:File:{filename}]]："
                    + "；".join(
                        f"{name}" for id, name in ress
                    )  # {construct_res_url(id)}
                )
                counts[-1] += 1
                # fmt: off
                booknav_wikitext = f"{{{{ZJLibBookNaviBar|prev={prev_filename or ""}|next={next_filename or ""}|parentresid={item['id']}|nth={i+1}|total={len(vols)}}}}}\n"
            wikitext = f"""=={{{{int:filedesc}}}}==
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

    for index in indices:
        index.append("")
        index.append("[[Category:Book in the Zhejiang Library]]")

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
                index = "\n".join(indices[i]) + "\n"
                assert (
                    cl := len(index.encode("utf-8"))
                ) <= 2 * 1024 * 1024, f"i={i}, cl={cl}"
                w.writerow(
                    [
                        f"Commons:Library_back_up_project/file_list/ZJLib/{i+1:02}",
                        index,
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
