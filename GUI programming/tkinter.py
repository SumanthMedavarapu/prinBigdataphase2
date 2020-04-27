
import tkinter as tk

from queries1 import *
 






 
root = tk.Tk()
root.title("PB charts visualization")
root.geometry('450x450')
button = tk.Button(root, text='query 1',width = 12, height = 1,command=main)
button.grid(row = 1, column = 2,padx = 10,pady = 1)
button = tk.Button(root, text='query 2',width = 12, height = 1,command=query2)
button.grid(row = 3, column = 2,padx = 10,pady = 1)
button = tk.Button(root, text='query 3',width = 12, height = 1,command=query3)
button.grid(row = 5, column = 2,padx = 10,pady = 1)
button = tk.Button(root, text='query 4',width = 12, height = 1,command=query4)
button.grid(row = 7, column = 2,padx = 10,pady = 1)
button = tk.Button(root, text='query 5',width = 12, height = 1,command=query5)
button.grid(row = 9, column = 2,padx = 10,pady = 1)
button = tk.Button(root, text='query 6',width = 12, height = 1,command=query6)
button.grid(row = 1, column = 4,padx = 10,pady = 1)
button = tk.Button(root, text='query 7',width = 12, height = 1,command=query7)
button.grid(row = 3, column = 4,padx = 10,pady = 1)
button = tk.Button(root, text='query 8',width = 12, height = 1,command=query8)
button.grid(row = 5, column = 4,padx = 10,pady = 1)
button = tk.Button(root, text='query 9',width = 12, height = 1,command=query9)
button.grid(row = 7, column = 4,padx = 10,pady = 1)
button = tk.Button(root, text='query 10',width = 12, height = 1,command=query10)
button.grid(row = 9, column = 4,padx = 10,pady = 1)



root.mainloop()        
 

 
 
 


