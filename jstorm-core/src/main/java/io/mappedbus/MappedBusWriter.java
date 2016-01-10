/* 
* Copyright 2015 Caplogic AB. 
* 
* Licensed under the Apache License, Version 2.0 (the "License"); 
* you may not use this file except in compliance with the License. 
* You may obtain a copy of the License at 
* 
* http://www.apache.org/licenses/LICENSE-2.0 
* 
* Unless required by applicable law or agreed to in writing, software 
* distributed under the License is distributed on an "AS IS" BASIS, 
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
* See the License for the specific language governing permissions and 
* limitations under the License. 
*/
package io.mappedbus;
import io.mappedbus.MappedBusConstants.Commit;
import io.mappedbus.MappedBusConstants.Length;
import io.mappedbus.MappedBusConstants.Structure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

/**
 * Class for writing messages to the bus.
 * <p>
 * Messages can either be message based or byte array based.
 * <p>
 * The typical usage is as follows:
 * <pre>
 * {@code
 * // Construct a writer
 * MappedBusWriter writer = new MappedBusWriter("/tmp/test", 100000L, 32, true);
 * writer.open();
 * 
 * // A: write an object based message
 * PriceUpdate priceUpdate = new PriceUpdate();
 *
 * writer.write(priceUpdate);
 * 
 * // B: write a byte array based message
 * byte[] buffer = new byte[32];
 *
 * writer.write(buffer, 0, buffer.length);
 *
 * // Close the writer
 * writer.close();
 * }
 * </pre>
 */
public class MappedBusWriter {
	private static Logger LOG = LoggerFactory.getLogger(MappedBusWriter.class);

	private MemoryMappedFile mem;
	
	private final String fileName;
	
	private final long fileSize;

	private final int entrySize;

	private final boolean append;

	private long timeout = 10000;

	private volatile long currentIndex = 0;

	private MemoryMappedFile sharedFile;

	/**
	 * Constructs a new writer.
	 * 
	 * @param fileName the name of the memory mapped file
	 * @param fileSize the maximum size of the file
	 * @param recordSize the maximum size of a record (excluding status flags and meta data)
	 * @param append whether to append to the file (will create a new file if false)
	 */
	public MappedBusWriter(String fileName, long fileSize, int recordSize, boolean append) {
		this.fileName = fileName;
		this.fileSize = fileSize;
		this.entrySize = recordSize + Length.RecordHeader;
		this.append = append;
	}
	
	/**
	 * Opens the writer.
	 *
	 * @throws IOException if there was an error opening the file
	 */
	public void open() throws IOException {
//		if (!append) {
//			new File(fileName).delete();
//		}
		try {
			mem = new MemoryMappedFile(fileName  + currentIndex, fileSize);
			sharedFile = new MemoryMappedFile(fileName + "_shared", MappedBusConstants.SHARED_FILE_SIE);
			// sharedFile.clearFile();
		} catch(Exception e) {
			throw new IOException("Unable to open the file: " + fileName, e);
		}
//		if (append) {
//			mem.compareAndSwapLong(Structure.Limit, 0, Structure.Data);
//		} else {
//			mem.putLongVolatile(Structure.Limit, Structure.Data);
//		}
	}

	/**
	 * Writes a message.
	 *
	 * @param message the message object to write
	 * @throws EOFException in case the end of the file was reached
	 */
	public void write(MappedBusMessage message) throws Exception {
		long limit = allocate();
		long commitPos = limit;
		limit += Length.StatusFlags;
		mem.putInt(limit, message.type());
		limit += Length.Metadata;
		message.write(mem, limit);
		commit(commitPos);
	}

	/**
	 * Writes a buffer of data.
	 *
	 * @param src the output buffer
	 * @param offset the offset in the buffer of the first byte to write
	 * @param length the length of the data
	 * @throws EOFException in case the end of the file was reached
	 */
	public synchronized boolean write(byte[] src, int offset, int length) throws Exception {
		long limit = allocate();
		while (limit < 0) {
			throw new RuntimeException("Failed to write, no space left and possibly reader not available to free space limit: " + limit);
		}
		long commitPos = limit;
		long readPos = commitPos + Length.Commit + Length.Rollback;
		// when the reader reads this it should set this flag
		// here we set it to 0
		writeSet(readPos);
		limit += Length.StatusFlags;
		mem.putInt(limit, length);
		limit += Length.Metadata;
		mem.setBytes(limit, src, offset, length);
		commit(commitPos);
		return true;
	}
	
	private long allocate() throws Exception {
		long time = System.currentTimeMillis();
		while (true) {
			long index = sharedFile.getLongVolatile(0);
			if (index > currentIndex) {
//				LOG.info("Alloc");
				nextFile(false);
//				LOG.info("Moved to next");
			} else if (index < currentIndex) {
				throw new RuntimeException("Index cannot go backward");
			}
			// if a writer write to the end of the file and reader reads content from the file
			// and deletes this index file during this time we are screwed.
			// this is highly unlikely to happen, but can happen
			long limit = mem.getAndAddLong(Structure.Limit, entrySize);
//			if (limit == 0) {
//				limit = mem.getAndAddLong(Structure.Limit, entrySize);
//			}
//			LOG.info("Limit: {}", limit);
			if (limit + entrySize < fileSize) {
				return limit;
			} else {
				MemoryMappedFile tmp = mem;
				// acquire the lock
				acquireLock();
				try {
					// lets try to update the currentIndex
					// if multiple writes try to do this only one should succeed
					boolean moved = sharedFile.compareAndSwapLong(0, currentIndex, currentIndex + 1);
					// now get the current index
					long currentIndexRead = sharedFile.getLongVolatile(0);
					// okay if I moved the index to the next, I should create the file and clear it
					if (moved || currentIndexRead > currentIndex) {
						currentIndex = currentIndexRead;
//						LOG.info("Create new file... " + fileName + (currentIndex));
						mem = new MemoryMappedFile(fileName + (currentIndex), fileSize);
                        if (moved) {
                            mem.putLongVolatile(Structure.Limit, Structure.Data);
							mem.putByteVolatile(Structure.Data, Commit.NotSet);
                        }
						// mem.clearFile();
					} else{
						currentIndex = currentIndexRead;
					}
				} finally {
					tmp.unmap();
					releaseLock();
				}
			}

			try {
				Thread.sleep(1);
			} catch (InterruptedException ignore) {
			}

			//LOG.info("Waiting....");
			if (System.currentTimeMillis() - time > timeout) {
				return -1;
			}
		}
	}

	public boolean nextFile(boolean reader) throws Exception {
//		LOG.info("Next.............................................................");
		// in reader case, because we only have one reader, we simply move to the next file
		if (reader) {
			// acquire the lock and read the index
			acquireLock();
			try {
				long memoryIndex = sharedFile.getLongVolatile(0);
				// check weather the writers have move forward
				if (memoryIndex > currentIndex) {
					mem.unmap();
					// now lets go to the next file
					mem = new MemoryMappedFile(fileName + (currentIndex + 1), fileSize);
					// okay we have read to the end and one of the writers have move to the next
					// lets delete the previous file
					if (currentIndex > 0) {
						File f = new File(fileName + (currentIndex - 1));
						boolean delete = f.delete();
					}
					currentIndex++;
					return true;
				} else {
					return false;
				}
			} finally {
				releaseLock();  // finally release the lock
			}
		} else {
			MemoryMappedFile tmp = mem;
			// acquire the lock
			acquireLock();
			try {
				// lets try to update the currentIndex
				// if multiple writes try to do this only one should succeed
				boolean moved = sharedFile.compareAndSwapLong(0, currentIndex, currentIndex + 1);
				// now get the current index
                long currentIndexRead = sharedFile.getLongVolatile(0);
//				LOG.info("current index: {}", currentIndex);
				// okay if I moved the index to the next, I should create the file and clear it
				if (moved || currentIndexRead > currentIndex) {
                    currentIndex = currentIndexRead;
//					LOG.info("Next file {}", fileName + currentIndex);
					mem = new MemoryMappedFile(fileName + (currentIndex), fileSize);
                    if (moved) {
                        mem.putLongVolatile(Structure.Limit, Structure.Data);
						mem.putByteVolatile(Structure.Data, Commit.NotSet);
                    }
				} else {
                    currentIndex = currentIndexRead;
                }
				return true;
			} finally {
				tmp.unmap();
				releaseLock();
			}
		}
	}

	public void acquireLock() {
		// wait until we acquire the lock
//		LOG.info("Aquiring lock");
		while (!sharedFile.compareAndSwapInt(8, 0, 1));
//		LOG.info("locked......");

	}

	public void releaseLock() {
		sharedFile.putIntVolatile(8, 0);
	}

	private void commit(long commitPos) {
		mem.putByteVolatile(commitPos, Commit.Set);
	}

	private void writeSet(long readPos) {
		mem.putByteVolatile(readPos, Commit.NotSet);
	}
	
	/**
	 * Closes the writer.
	 *
	 * @throws IOException if there was an error closing the file
	 */
	public void close() throws IOException {
		try {
			mem.unmap();
		} catch(Exception e) {
			throw new IOException("Unable to close the file", e);
		}
	}
}