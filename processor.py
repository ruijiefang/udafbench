import logging as log
import sys
import os
from tree_sitter import Language, Parser
import tree_sitter_scala as tsscala
import tree_sitter_java as tsjava

#log.basicConfig(level=log.DEBUG, format='|> %(message)s')
log.basicConfig(level=log.WARNING, format='|> %(message)s')

CODING = 'utf-8'

# Load the Scala language from your built shared library.
SCALA_LANGUAGE = Language(tsscala.language())
JAVA_LANGUAGE = Language(tsjava.language())

stats_loc = None
stats_control_flow = None
stats_lang = None

def printl(*args):
  args = [str(x) for x in args]
  log.info(' '.join(args))

def traverse(node):
    """Yield the given node and all its descendants."""
    yield node
    for child in node.children:
        yield from traverse(child)

def totals(needle, haystack):
    return sum(map(lambda c : 1 if needle in c else 0, haystack)) 


def gettext(node):
    return node.text.decode(CODING)

def count_line_numbers(node, func_name):
    if node.type == 'function_definition':
        for child in node.children:
            if node.type == "comment":
                continue
            text = gettext(child).rstrip().lstrip()
            if text.startswith('{') and text.endswith('}'):
                return totals('\n', text)
    return 0

def has_nontrivial_control_flow(node):
    non_trivial_types = {
        "if_expression", "if_statement",
        "while_expression", "while_statement",
        "for_expression", "for_statement",
        "call_expression", "function_call", "method_call", "method_invocation",
        "tuple_expression", "tuple_type"
    }
    if node.type in non_trivial_types:
        return True
    else:
        for child in node.children:
            if node.type == "comment":
                continue
            if has_nontrivial_control_flow(child):
                return True
        return False


def assert_func_exists(node, func_name, control_flow=True):
    global stats_loc
    global stats_control_flow
    printl('------------------------------------------')
    printl('       at child node inside class A: ') # , str(node))
    printl('       text: ', node.text.decode('utf-8'))
    if node.type == "function_definition" or node.type == "method_declaration":
        printl('       ** node is a definition')
        retval = False
        # print('total number of lines in function: ', count_line_numbers(node, func_name))
        for child in node.children:
            if node.type == "comment":
                continue
            printl('               ------  at child of node inside class A: ') #, str(node))
            printl('                 text: ', child.text.decode('utf-8'))
            if child.type == "identifier":
                printl('    ********** is an identifier')
                if child.text.decode('utf-8') == func_name:
                    printl('          %%%%% done')
                    retval = True
        if retval == True:
            # compute stats
            stats_loc = count_line_numbers(node, func_name)
            if control_flow:
                r = has_nontrivial_control_flow(node)
                printl(' ------ has non-trivial control flow?', r)
                stats_control_flow = r
                return r
            else:
                return True  
    for child in node.children:
        if assert_func_exists(child, func_name, control_flow=control_flow):
            return True
    return False

def class_implements(node, impls):
    for child in node.children:
        if node.type == "comment":
            continue
        for i in impls:
            text = gettext(child).rstrip().lstrip()
            if text.startswith('extends') and i in text:
                printl('NOTE: Class implements', impls)
                return True
    for child in node.children:
        if class_implements(child, impls):
            return True
    return False


def has_class_with(code, parser, impls, func_name):
    """
    Parses the Scala code and returns True if there is a class definition
    for a given class that contains a method named func_name.
    """
    tree = parser.parse(bytes(code, "utf8"))
    root = tree.root_node

    printl('at root node: ', str(root))
    printl('text: ', str(root.text))
    for node in traverse(root):
#        printl('  ** traversing node ', node.text.decode(CODING))
        if node.type == "comment" or node.type == "line_comment":
            continue
        if node.type == "class_definition" or node.type == "class_declaration":
            # Look for an identifier child with the value "A"
            printl(' at class definition: ', str(node))
            printl(' text: ', node.text.decode('utf-8'))
            if not(class_implements(node, impls)):
                printl('NOTE: Class does not implement ', impls)
                continue
            for child in node.children:
                printl('    at child of class: ', str(child))
                printl('     text: ', child.text.decode('utf-8'))
                if child.type == "identifier" :
                    printl(' *-*-* found class A *-*-*')
                    # Found class A; now search its subtree for a method 'foo'
                    if assert_func_exists(node, func_name, control_flow=True):
                        return True
                    else:
                        printl('ERR: cannot find ', func_name, ' in proper class')

    return False


def main():
    if len(sys.argv) < 2:
        printl("Usage: python script.py <file1.scala> <file2.scala> ...")
        sys.exit(1)


    filepath=sys.argv[1]
    class_name = sys.argv[2]
    method_name = sys.argv[3]

    if not os.path.isfile(filepath):
        print('error: argv[1] = ', filepath, ', which is not a filepath')
        exit(1)
    with open(filepath, 'r', encoding='utf-8') as f:
        code = f.read()
    parser = None
        
    if '.java' in filepath:
        parser = Parser()
        parser.language = JAVA_LANGUAGE
        stats_lang = 'JAVA'
    else:
        parser = Parser()
        parser.language = SCALA_LANGUAGE
        stats_lang = 'SCALA'

    if has_class_with(code, parser, [class_name], method_name): # ['Aggregator'], 'reduce'):
        print(f"{filepath} | GOOD | LOC={stats_loc} | CONTROLFLOW={stats_control_flow} | LANG={stats_lang}")
    else:
        print(f'{filepath} | BAD | LOC={stats_loc} | CONTROLFLOW={stats_control_flow} | LANG={stats_lang}')

if __name__ == "__main__":
    main()
