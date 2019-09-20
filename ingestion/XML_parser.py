from lxml import etree


def parse_posts_gen(fp):

    context = etree.iterparse(fp, events=('end',))
    for action, elem in context:
        if elem.tag == 'row':
            assert elem.text is None, "The row wasn't empty"
            yield elem.attrib
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


