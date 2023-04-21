from bs4 import NavigableString


def text_between(from_, until):
    result = ''
    while from_ and from_ != until:
        if isinstance(from_, NavigableString):
            text = from_.strip()
            if len(text):
                result += text
        from_ = from_.next
    return result
