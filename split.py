
import multiprocessing
import os
import tkinter as tk
import sys


import pandas as pd
import queue

from tkinter import (Tk, BOTH,
                     NORMAL, DISABLED)
from tkinter.ttk import Frame

from multiprocessing import Process, Queue
from queue import Empty

FILE_TYPE = ['excel', 'csv']
fields = [['File Path', None], ['Chunk Size', 1000]]
queue = Queue()
message = None
DELAY1 = 80
DELAY2 = 20


def splitter(queue, file, chunk_size):
    if float(chunk_size) <= 0:
        message = "Chunk size should be larger than 0"
        queue.put(message)
        exit()
    if os.path.isfile(file):
        extension = os.path.splitext(file)[1]
        if extension == '.xlsx':
            file_data = pd.read_excel(file)
            count = file_data.shape[0]
            if chunk_size != 0:
                no_of_file = count//chunk_size + 1
            else:
                no_of_file = 1
            for i in range(no_of_file):
                file_path = file.replace(extension, "_{}{}".format(i, extension))
                file_data[i*chunk_size:(i+1)*chunk_size].to_excel(file_path, index=False)
        elif extension == '.csv':
            file_data = pd.read_csv(file)
            count = file_data.shape[0]
            if chunk_size != 0:
                no_of_file = count//chunk_size + 1
            else:
                no_of_file = 1
            for i in range(no_of_file):
                file_path = file.replace(extension, "_{}{}".format(i, extension))
                file_data[i*chunk_size:(i+1)*chunk_size].to_csv(file_path, index=False)


class Method(Frame):

    def __init__(self, parent):
        Frame.__init__(self, parent, name="frame")

        self.parent = parent
        self.init_ui()

    def init_ui(self):
        self.parent.title("File Splitter")
        self.pack(fill=BOTH, expand=True)
        self.grid_columnconfigure(4, weight=1)
        self.grid_rowconfigure(3, weight=1)
        self.entries = {}
        self.labels = {}
        self.frame = None
        self.split_button = None
        self.quit_button = None
        self.initial_parent = self.parent
        self.on_choose()
        row = tk.Frame(self.initial_parent)
        self.message_box = None
        self.rowconfigure(4, pad=2)
        self.destination = None

    def make_form(self, fields):
        self.destroy_form()

        for field in fields:
            print(field)
            row = tk.Frame(self.initial_parent)
            lab = tk.Label(row, width=22, text=field[0] + ": ", anchor='w')
            ent = tk.Entry(row)
            row.pack(side=tk.TOP,
                     fill=tk.X,
                     padx=5,
                     pady=5)
            lab.pack(side=tk.LEFT)
            ent.pack(side=tk.RIGHT,
                     expand=tk.YES,
                     fill=tk.X)
            if field[1]:
                ent.insert(1, field[1])
            self.entries[field[0]] = ent
            self.labels[field[0]] = lab
        row = tk.Frame(self.initial_parent)
        row.pack(side=tk.TOP,
                 fill=tk.X,
                 padx=5,
                 pady=5)
        lab = tk.Label(row, width=50, text="", anchor='w', fg='blue')
        lab.pack(side=tk.LEFT)
        self.labels['Status'] = lab

    def destroy_form(self):
        for i in range(len(self.parent.children)*100):
            if i ==0 and self.parent.children.get('!frame'):
                self.parent.children.get('!frame').destroy()
            elif self.parent.children.get('!frame{}'.format(i+1)):
                self.parent.children.get('!frame{}'.format(i+1)).destroy()
            if i ==0 and self.parent.children.get('!button'):
                self.parent.children.get('!button').destroy()
            elif self.parent.children.get('!button{}'.format(i+1)):
                self.parent.children.get('!button{}'.format(i+1)).destroy()

    def on_choose(self):
        self.destroy_form()
        field = fields
        self.make_form(field)
        self.buttons()

    def on_get_value(self):
        if self.p1.is_alive():
            self.after(DELAY1, self.on_get_value)
            if not queue.empty():
                self.labels['Status']['text'] = queue.get(0)
            return
        elif "0" not in self.labels['Status']['text']:
            try:
                self.labels['Status']['text'] = ''
            except Empty:
                print("queue is empty")
        else:
            self.split_button.config(state=NORMAL)

    def buttons(self):
        self.split_button = tk.Button(self.parent, text='Start', fg='blue',
                       command=(lambda e=self.entries: self.base_method(e)))
        self.split_button.pack(side=tk.LEFT, padx=5, pady=5)
        self.quit_button = tk.Button(self.parent, text='Quit', command=self.parent.quit, fg='red')
        self.quit_button.pack(side=tk.LEFT, padx=5, pady=5)

    def base_method(self, entries):
        self.split_button.config(state=DISABLED)
        file_path = entries['File Path'].get()
        try:
            chunk_size = int(entries['Chunk Size'].get())
        except ValueError:
            message = "Chunk size should be larger than 0"
            queue.put(message)
            exit()
        self.p1 = Process(target=splitter, args=(
            queue, file_path, chunk_size))
        self.p1.start()
        self.after(DELAY1, self.on_get_value)


def main():
    root = Tk()
    app = Method(root)
    root.mainloop()


if __name__ == '__main__':
    if sys.platform.startswith('win'):
        multiprocessing.freeze_support()
    main()
