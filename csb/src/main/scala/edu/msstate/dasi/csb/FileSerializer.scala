package edu.msstate.dasi.csb

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

/**
 * Provides object serialization features using files.
 */
object FileSerializer {
  /**
   * Loads an object from file.
   *
   * @param filename the filename of the source file
   * @tparam T the type of the object to load
   * @return the loaded object
   */
  def load[T](filename: String): T = {
    val ois = new ObjectInputStream(new FileInputStream(filename))
    val obj = ois.readObject().asInstanceOf[T]
    ois.close()

    obj
  }

  /**
   * Saves an object to file.
   *
   * @param obj      the object to save
   * @param filename the filename of the destination file
   */
  def save(obj: Any, filename: String): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(filename))
    oos.writeObject(obj)
    oos.close()
  }
}
