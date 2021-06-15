/*
 * RamSort.hpp
 *
 *  Created on: May 23, 2021
 *      Author: mad
 */

#ifndef INCLUDE_CHIA_RAMSORT_HPP_
#define INCLUDE_CHIA_RAMSORT_HPP_

#include <chia/RamSort.h>
#include <chia/util.hpp>

#include <map>
#include <algorithm>
#include <unordered_map>

template<typename T, typename Key>
void RamSort<T, Key>::bucket_t::write(const void* data_, size_t count)
{
	std::lock_guard<std::mutex> lock(mutex);
	const auto last_size = num_entries;
	resize(last_size*T::disk_size+count*T::disk_size);
	memcpy(data.get()+last_size*T::disk_size, data_, count*T::disk_size);
	num_entries += count;
}

template<typename T, typename Key>
void RamSort<T, Key>::bucket_t::remove()
{
	data.reset();
}

template<typename T, typename Key>
void RamSort<T, Key>::bucket_t::resize(const size_t newSize)
{
	if(newSize==0){
		data.reset();
	}else{
		uint8_t* new_ptr = static_cast<uint8_t*>(std::realloc(data.get(), newSize));
		if(!new_ptr){
			throw std::bad_alloc();
		}
		data.release();
		data.reset(new_ptr);
	}
}

template<typename T, typename Key>
RamSort<T, Key>::WriteCache::WriteCache(RamSort* ram, int key_shift, int num_buckets)
	:	ram(ram), key_shift(key_shift), buckets(num_buckets)
{
}

template<typename T, typename Key>
void RamSort<T, Key>::WriteCache::add(const T& entry)
{
	const size_t index = Key{}(entry) >> key_shift;
	if(index >= buckets.size()) {
		throw std::logic_error("bucket index out of range");
	}
	auto& buffer = buckets[index];
	if(buffer.count >= buffer.capacity) {
		ram->write(index, buffer.data, buffer.count);
		buffer.count = 0;
	}
	entry.write(buffer.entry_at(buffer.count));
	buffer.count++;
}

template<typename T, typename Key>
void RamSort<T, Key>::WriteCache::flush()
{
	for(size_t index = 0; index < buckets.size(); ++index) {
		auto& buffer = buckets[index];
		if(buffer.count) {
			ram->write(index, buffer.data, buffer.count);
			buffer.count = 0;
		}
	}
}

template<typename T, typename Key>
RamSort<T, Key>::RamSort(	int key_size, int log_num_buckets,
							std::string file_prefix, bool read_only)
	:	key_size(key_size),
		log_num_buckets(log_num_buckets),
		bucket_key_shift(key_size - log_num_buckets),
		keep_files(read_only),
		is_finished(read_only),
		cache(this, key_size - log_num_buckets, 1 << log_num_buckets),
		buckets(1 << log_num_buckets)
{
	for(size_t i = 0; i < buckets.size(); ++i) {
		auto& bucket = buckets[i];
		bucket.file_name = file_prefix + ".sort_bucket_" + std::to_string(i) + ".tmp"; // necessary?
		bucket.resize(0);
		if(read_only) {
			throw std::runtime_error("RamSort can't use as readonly");
		}
	}
}

template<typename T, typename Key>
void RamSort<T, Key>::add(const T& entry)
{
	cache.add(entry);
}

template<typename T, typename Key>
void RamSort<T, Key>::write(size_t index, const void* data, size_t count)
{
	if(is_finished) {
		throw std::logic_error("read only");
	}
	if(index >= buckets.size()) {
		throw std::logic_error("bucket index out of range");
	}
	buckets[index].write(data, count);
}

template<typename T, typename Key>
std::shared_ptr<typename RamSort<T, Key>::WriteCache> RamSort<T, Key>::add_cache()
{
	return std::make_shared<WriteCache>(this, bucket_key_shift, buckets.size());
}

template<typename T, typename Key>
void RamSort<T, Key>::read(Processor<std::pair<std::vector<T>, size_t>>* output,
							int num_threads, int num_threads_read)
{
	if(num_threads_read < 0) {
		num_threads_read = std::max(num_threads / 2, 2);
	}
	
	ThreadPool<	std::pair<std::vector<T>, size_t>,
				std::pair<std::vector<T>, size_t>> sort_pool(
		[](std::pair<std::vector<T>, size_t>& input, std::pair<std::vector<T>, size_t>& out, size_t&) {
			std::sort(input.first.begin(), input.first.end(),
				[](const T& lhs, const T& rhs) -> bool {
					return Key{}(lhs) < Key{}(rhs);
				});
			out = std::move(input);
		}, output, num_threads, "Ram/sort");
	
	Thread<std::vector<std::pair<std::vector<T>, size_t>>> sort_thread(
		[&sort_pool](std::vector<std::pair<std::vector<T>, size_t>>& input) {
			for(auto& block : input) {
				sort_pool.take(block);
			}
		}, "Ram/sort");
	
	ThreadPool<	std::pair<size_t, size_t>,
				std::vector<std::pair<std::vector<T>, size_t>>> read_pool(
		std::bind(&RamSort::read_bucket, this,
				std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
		&sort_thread, num_threads_read, "Ram/read");
	
	uint64_t offset = 0;
	for(size_t i = 0; i < buckets.size(); ++i) {
		read_pool.take_copy(std::make_pair(i, offset));
		offset += buckets[i].num_entries;
	}
	read_pool.close();
	sort_thread.close();
	sort_pool.close();
}

template<typename T, typename Key>
void RamSort<T, Key>::read_bucket(	std::pair<size_t, size_t>& index,
									std::vector<std::pair<std::vector<T>, size_t>>& out, size_t&)
{
	auto& bucket = buckets[index.first];
	
	const int key_shift = bucket_key_shift - log_num_buckets;
	if(key_shift < 0) {
		throw std::logic_error("key_shift < 0");
	}
	std::unordered_map<size_t, std::vector<T>> table;
	table.reserve(size_t(1) << log_num_buckets);

	const uint8_t* ptr = bucket.data.get();
	
	for(size_t i = 0; i < bucket.num_entries; ++i)
	{
		T entry;
		entry.read(ptr+i*T::disk_size);
		
		auto& block = table[Key{}(entry) >> key_shift];
		if(block.empty()) {
			block.reserve((bucket.num_entries >> log_num_buckets) * 1.1);
		}
		block.push_back(entry);
	}
	if(!keep_files) {
		bucket.remove();
	}
	
	std::map<size_t, std::vector<T>> sorted;
	for(auto& entry : table) {
		sorted.emplace(entry.first, std::move(entry.second));
	}
	table.clear();
	
	out.reserve(sorted.size());
	uint64_t offset = index.second;
	for(auto& entry : sorted) {
		const auto count = entry.second.size();
		out.emplace_back(std::move(entry.second), offset);
		offset += count;
	}
}

template<typename T, typename Key>
void RamSort<T, Key>::finish()
{
	cache.flush();
	is_finished = true;
}

template<typename T, typename Key>
void RamSort<T, Key>::close()
{
	for(auto& bucket : buckets) {
		if(!keep_files) {
			bucket.remove();
		}
	}
	buckets.clear();
}


#endif /* INCLUDE_CHIA_RAMSORT_HPP_ */
