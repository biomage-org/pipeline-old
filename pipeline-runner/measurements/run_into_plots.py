import matplotlib.pyplot as plt
import subprocess

def get_peak_memory_consumption(number):
  bashCommand = f"mprof peak mprof_run_for_num_to_merge_{number}"
  process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
  output, error = process.communicate()

  memoryConsumed = output.decode("utf-8").split("\t")[1].strip(" MiB\n")
  
  return float(memoryConsumed)

cells = 918
numbers_to_merge = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]

amount_of_cells = []
amount_of_memory = []

for number in numbers_to_merge:
  amount_of_cells.append(cells * number)

  memory = get_peak_memory_consumption(number)
  amount_of_memory.append(memory)

plt.ylabel("Memory consumed (MiB)")
plt.xlabel("# Cells")
plt.plot(amount_of_cells, amount_of_memory)
plt.show()