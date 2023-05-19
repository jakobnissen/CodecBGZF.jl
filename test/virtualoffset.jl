voff = VirtualOffset(0, 0)
@test voff == VirtualOffset(0, 0)
@test voff != VirtualOffset(0, 1)
@test voff <= VirtualOffset(0, 1)
@test voff != VirtualOffset(1, 0)
@test voff <= VirtualOffset(1, 0)
@test UInt64(VirtualOffset(0, 1)) == UInt64(1)
@test UInt64(VirtualOffset(1, 0)) == UInt(1 << 16)
@test VirtualOffset(UInt64(0x10001)) == VirtualOffset(1, 1)
@test isless(voff, VirtualOffset(1, 0))
@test voff[1] == 0
@test voff[2] == 0
@test string(voff) == "VirtualOffset(0, 0)"

voff += 1
@test voff[1] == 0
@test voff[2] == 1

voff = VirtualOffset(1234, 555)
@test voff[1] == 1234
@test voff[2] == 555
@test string(voff) == "VirtualOffset(1234, 555)"

voff = VirtualOffset(1234, 555)
buf = IOBuffer()
@test write(buf, voff) == sizeof(UInt64)
seekstart(buf)
@test read(buf, VirtualOffset) === voff
@test eof(buf)

@test_throws ArgumentError VirtualOffset(1 << 48, 0)
@test_throws ArgumentError VirtualOffset(0, 1 << 16)