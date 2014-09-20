import sys
import os
import re
import subprocess
import struct
import datetime
from uuid import UUID
from collections import namedtuple
from operator import attrgetter

"""Ideally this whole thing is written in native rust,
including the part where the application calls ioctls to
get the serialised btrfs stream out"""


class BtrfsListRecord(namedtuple('BtrfsListRecord', [
        'id', 'gen', 'top_level', 'parent_uuid', 'uuid', 'path'
])):
    class List(list):
        @classmethod
        def from_lines(cls, lines):
            return cls(map(BtrfsListRecord.from_line, lines))

        @classmethod
        def load_from(cls, path):
            subproc = subprocess.Popen([
                'btrfs', 'subv', 'list', path, '-uqt'],
                stdout=subprocess.PIPE)
            lines = subproc.stdout.xreadlines()
            next(lines)  # header
            next(lines)  # sep
            return cls.from_lines((x.strip() for x in lines))

        def find_latest(self, uuid_list):
            candidates = list()
            for record in self:
                if UUID(record.uuid) in uuid_list:
                    candidates.append((int(record.gen), record))
            if not candidates:
                return None
            return sorted(candidates, reverse=True)[0][1]

        def find_snapshots_of_path(self, path):
            for record in self:
                if record.path == path:
                    path_record = record
                    break
            else:
                raise KeyError("No path `{}' found".format(path))
            return [
                record for record in self
                if record.parent_uuid == path_record.uuid]

    @classmethod
    def from_line(cls, line):
        try:
            return cls(*re.compile(r'\t+').split(line))
        except TypeError:
            raise ValueError("invalid line: ``{}''".format(line))


class ProtocolClient(object):
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def _handshake(self):
        self.writer.write('\xa8\x5b\x4b\x2b\x1b\x75\x4c\x0a')
        assert self.reader.read(8) == b"\xfb\x70\x4c\x63\x41\x1d\x9c\x0a"

    def list_nodes(self):
        self.writer.write(struct.pack('>Q', 2))
        out = list()
        while True:
            if '\x00' == self.reader.read(1):
                break
            out.append(UUID(bytes=self.reader.read(16)))
        return out

    def find_nodes(self, uuid_list):
        self.writer.write(struct.pack('>QI', 1, len(uuid_list)))
        for uuid in uuid_list:
            self.writer.write(UUID(uuid).bytes)
        out = list()
        while True:
            if '\x00' == self.reader.read(1):
                break
            out.append(UUID(bytes=self.reader.read(16)))
        return out

    def get_graph(self):
        self.writer.write(struct.pack('>Q', 4))
        (len_,) = struct.unpack(
            '>I',
            self.reader.read(struct.calcsize('>I')))
        print("graph_response: {!r}".format(self.reader.read(len_)))

    def upload_archive(self):
        self.writer.write(struct.pack('>Q', 3))
        return self.writer

    def exit(self):
        self.writer.write(struct.pack('>Q', 0))


def upload_bogus_archives(client):
    writer = client.upload_archive()
    writer.write(
        b"reliable-encap\x00\x00\x00\x00\xe3\xb0\xc4B\x98\xfc\x1c\x14"
        b"\x9a\xfb\xf4\xc8\x99o\xb9$'\xaeA\xe4d\x9b\x93L\xa4\x95\x99"
        b"\x1bxR\xb8U\xe3\xb0\xc4B\x98\xfc\x1c\x14\x9a\xfb\xf4\xc8\x99"
        b"o\xb9$'\xaeA\xe4d\x9b\x93L\xa4\x95\x99\x1bxR\xb8U")

    writer = client.upload_archive()
    writer.write(
        b"reliable-encap\x00\x00\x00\nfoobarbaz\n/r\xcc\x11\xa6\xfc\xd0'"
        b"\x1e\xce\xf8\xc6\x10V\xee\x1e\xb1$;\xe3\x80[\xf9\xa9\xdf\x98"
        b"\xf9/v6\xb0\\\x00\x00\x00\x00/r\xcc\x11\xa6\xfc\xd0'\x1e\xce"
        b"\xf8\xc6\x10V\xee\x1e\xb1$;\xe3\x80[\xf9\xa9\xdf\x98\xf9/v6\xb0"
        b"\\/r\xcc\x11\xa6\xfc\xd0'\x1e\xce\xf8\xc6\x10V\xee\x1e\xb1$;\xe3"
        b"\x80[\xf9\xa9\xdf\x98\xf9/v6\xb0\\")


def main(argv):
    (subv_root, subv_name) = sys.argv[1:3]
    recs = BtrfsListRecord.List.load_from(subv_root)
    parent_candidates = recs.find_snapshots_of_path(subv_name)
    popen = subprocess.Popen([
        'ssh', '-i', '/root/backups.id_rsa',
        'sell@vita.yasashiisyndicate.org',
        'backupserver', '/nowhere'],
        stdout=subprocess.PIPE, stdin=subprocess.PIPE)

    client = ProtocolClient(popen.stdout, popen.stdin)
    client._handshake()

    print("listing nodes: ")
    remote_nodes = client.list_nodes()
    for node in remote_nodes:
        print("    node: {}".format(node))

    remote_candidates = client.find_nodes(
        map(attrgetter('uuid'), parent_candidates))
    print("finding nodes in {!r}:".format(parent_candidates))
    for node in remote_candidates:
        print("    node: {}".format(node))

    good_parent = recs.find_latest(remote_candidates)
    print("Found a good parent: {!r}".format(good_parent))

    client.get_graph()
    __import__('sys').exit()
    archive = client.upload_archive()
    snapshot_name = '{}_{}' \
        .format(subv_name, datetime.datetime.now()) \
        .replace(' ', 'T')
    assert subprocess.Popen([
        'btrfs', 'subvolume', 'snapshot', '-r',
        os.path.join(subv_root, subv_name),
        os.path.join(subv_root, snapshot_name)
    ]).wait() == 0
    assert subprocess.Popen(['sync']).wait() == 0
    if good_parent:
        sp_btrfs = subprocess.Popen([
            '/root/bin/reliable-encap', '--', 'btrfs', 'send', '-p',
            os.path.join(subv_root, good_parent.path),
            os.path.join(subv_root, snapshot_name),
        ], stdout=subprocess.PIPE)
    else:
        sp_btrfs = subprocess.Popen([
            '/root/bin/reliable-encap', '--', 'btrfs', 'send',
            os.path.join(subv_root, snapshot_name),
        ], stdout=subprocess.PIPE)
    while True:
        buf = sp_btrfs.stdout.read(2 ** 16)
        if not buf:
            break
        archive.write(buf)
    sp_btrfs.wait()

    client.exit()
    popen.wait()

if __name__ == '__main__':
    main(sys.argv)


