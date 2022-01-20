#include <iostream>
#include "mds/mdstypes.h"

using namespace std;

int main(int argc, char *argv[])
{
	adsl::LoadArray_Int arr(5);
	cout << arr << std::endl;

	arr.shift(2, 10);
	cout << arr << std::endl;

	arr.shift(1, 20);
	cout << arr << std::endl;

	arr.shift(1, 30);
	cout << arr << std::endl;

	return 0;
}
