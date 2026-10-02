"""Inline diagrams.net SVG image assets so GitHub's SVG CSP can display them."""

from pathlib import Path
from sys import argv
from urllib.parse import unquote
from xml.etree import ElementTree as ET

SVG = "http://www.w3.org/2000/svg"
XLINK = "http://www.w3.org/1999/xlink"
ET.register_namespace("", SVG)
ET.register_namespace("xlink", XLINK)


def flatten(path: Path) -> int:
    tree = ET.parse(path)
    count = 0

    for parent in tree.getroot().iter():
        for index, image in enumerate(list(parent)):
            if image.tag != f"{{{SVG}}}image":
                continue
            href = image.get(f"{{{XLINK}}}href", image.get("href", ""))
            if not href.startswith("data:image/svg+xml,"):
                continue

            icon = ET.fromstring(unquote(href.partition(",")[2]))
            embedded = ET.Element(
                f"{{{SVG}}}svg",
                {
                    "x": image.get("x", "0"),
                    "y": image.get("y", "0"),
                    "width": image.get("width", icon.get("width", "0")),
                    "height": image.get("height", icon.get("height", "0")),
                    "viewBox": icon.get("viewBox", ""),
                },
            )
            for key, value in icon.attrib.items():
                if key not in {"x", "y", "width", "height", "viewBox"}:
                    embedded.set(key, value)
            embedded.extend(list(icon))
            parent.remove(image)
            parent.insert(index, embedded)
            count += 1

    if count:
        tree.write(path, encoding="unicode", xml_declaration=True)
    return count


if __name__ == "__main__":
    for filename in argv[1:]:
        path = Path(filename)
        print(f"{path}: inlined {flatten(path)} icons")
