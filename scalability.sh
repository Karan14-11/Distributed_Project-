make first &


for ((i=1; i<=100; i++))
do
    echo "Starting server iteration $i"
    make server &
    # sleep 1
done