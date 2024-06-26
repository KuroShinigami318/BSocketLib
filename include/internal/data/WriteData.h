#pragma once

#include <vector>

namespace internal
{
namespace data
{
struct WriteData
{
	using bytes_t = std::vector<char>;
	bytes_t& bytes;
	bytes_t::iterator begin;
	bytes_t::iterator end;
	size_t size;
};
}
}