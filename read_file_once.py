file_read_path = 'read_file'

file_size = 100
byte_size = 1

output_arr = []
with open(file_read_path, 'rb', 0) as f:
    for _ in range(int(file_size / byte_size)):
        read_byte = f.read(byte_size)
        output_arr.append(read_byte)

print(output_arr)
print('총 개수:', len(output_arr))
