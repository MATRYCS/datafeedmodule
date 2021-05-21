def solution(s):
    temp_list = []
    result = []
    if s:
        for i in range(len(s)):
            letter = s[i]
            if not letter.isupper():
                temp_list.append(letter)
            else:
                result.append("".join(temp_list))
                temp_list = []
                temp_list.append(letter)
        result.append("".join(temp_list))
        result = [word for word in result if word != ""]
        return " ".join(result)
    else:
        return ""


def count(string):
    # The function code should be here
    string_dict = {}
    if string:
        for char in string:
            if char in string_dict.keys():
                string_dict[char] +=1
            else:
                string_dict[char] = 1
    return string_dict

def clean_string(s):
    if s:
        inverted_str_ = s[::-1]
        back_space = 0
        result_str_ = ""
        for i in range(len(inverted_str_)):
            char = inverted_str_[i]
            if char == "#":
                back_space +=1
            else:
                if back_space:
                    back_space -=1
                    continue
                else:
                    result_str_ +=char
        return result_str_[::-1]
    else:
        return ""


def duplicate_count(text):
    text_lower = text.lower()
    text_dict = {}
    output_list = []
    for char in text_lower:
        if char in text_dict.keys():
            text_dict[char] += 1
            if text_dict[char] > 1 and char not in output_list:
                output_list.append(char)
        else:
            text_dict[char] = 1

    return len(output_list)


# time: 45 minutes
# There are three types of edits that can be performed on strings:
# insert a character, remove a character, or replace a character.
# Given two strings, write a method to check if they are
# one edit (or zero edits) away

def one_away(first, second):
    different_letters = 0

    first_word_length = len(first)
    second_word_length = len(second)

    if abs(first_word_length - second_word_length) == 0:

        for i in range(len(first)):
            first_letter = first[i]
            second_letter = second[i]
            if first_letter != second_letter:
                different_letters += 1

        if different_letters == 1 or different_letters == 0:
            return True
        else:
            return False

    elif abs(first_word_length - second_word_length) == 1:
        max_len = max(first_word_length, second_word_length)

        if first_word_length > second_word_length:
            big_word = first
            small_word = second
        else:
            big_word = second
            small_word = first

        for i in range(max_len - 1):
            first_letter = first[i]
            second_letter = second[i]

            if first_letter != second_letter:
                different_letters += 1

                fist_substring = big_word[i - 1:]
                second_substring = small_word[i:]

                if fist_substring == second_substring:
                    return True

        if different_letters == 1 or different_letters == 0:
            return True
        else:
            return False
    else:
        return False


assert (one_away("pale", "bale") == True)  # 1
assert (one_away("blue", "grey") == False)  # 2
assert (one_away("abcd", "dcba") == False)  # 3
assert (one_away("pales", "pale") == True)  # 4
assert (one_away("palle", "pale") == True)  # 5
assert (one_away("ple", "pale") == False)  # 6
assert (one_away("pale", "bae") == False)  # 7
assert (one_away("pale", "ball") == False)  # 8
assert (one_away("paless", "pale") == False)  # 9

print("good job!")

solution("CamelCase")