import random

def generate_tuples(num_tuples, max_branch_id, max_amount):
    tuples_list = [(random.randint(1, max_branch_id), random.randint(1, max_amount)) for _ in range(num_tuples)]
    return tuples_list

num_tuples = 5
max_branch_id = 10
max_amount = 1000

result = generate_tuples(num_tuples, max_branch_id, max_amount)
print(result)

print(3)