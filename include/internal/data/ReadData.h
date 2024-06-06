#pragma once

#include <vector>

class ISocket;

namespace internal
{
namespace data
{
struct ReadData
{
	std::vector<char>& bytes;
	ISocket& socket;
};
}
}