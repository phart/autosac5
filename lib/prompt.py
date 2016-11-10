"""
prompt.py

A collection of functions for prompting the user for input.

Copyright (C) 2016  Nexenta Systems
William Kettler <william.kettler@nexenta.com>
"""


def prompt_continue():
    """
    Prompt the user to hit ENTER to continue.

    Args:
        None
    Returns:
        None
    """
    input("Please hit ENTER to continue...")


def prompt(question, answers):
    """
    Prompt the user with a question and only accept defined answers.

    Args:
        question (str): Question string
        answers (list): A list containing accpeted response value
    Returns:
        answer (str|int): Provided answer
    """
    print(question)

    # print a numbered list of answers
    for i in range(len(answers)):
        print(' %d. %s' % (i+1, answers[i]))

    while True:
        # Prompt the user for an integer
        try:
            choice = int(input(">>> "))
        # A ValueError is raised if the user does not provide an integer
        except ValueError:
            print("Invalid input.")
            continue

        # Confirm the integer is positive
        if choice < 1:
            print("Invalid input.")
            continue

        # Confirm the provided integer is not out of range
        try:
            answer = answers[choice-1]
        # An IndexError is raise when the list index is out of range
        except IndexError:
            print("Invalid input.")
            continue

        # If we make it here the answer is valid
        break

    return answer


def prompt_yn(question):
    """
    Prompt the user with a yes or no question.

    Args:
        question (str): Question string
    Returns:
        answer (bool): Answer True/False
    """
    while True:
        choice = input("%s [y|n] " % question)
        if choice == "y":
            answer = True
            break
        elif choice == "n":
            answer = False
            break
        else:
            print("Invalid input.")

    return answer
