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
    val ois = new ObjectInputStream(new FileInputStream(filename)) {
      // Defines a custom class loader to handle Scala instances
      override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
        try { Class.forName(desc.getName, false, getClass.getClassLoader) }
        catch { case _: ClassNotFoundException => super.resolveClass(desc) }
      }
    }

    val obj = ois.readObject().asInstanceOf[T]
    ois.close()

    obj
  }

  /**
   * Saves an object to file.
   *
   * @param obj      the object to save
   * @param filename the filename of the destination file
   * @tparam T the type of the object to save
   */
  def save[T](obj: T, filename: String): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(filename))
    oos.writeObject(obj)
    oos.close()
  }
}
