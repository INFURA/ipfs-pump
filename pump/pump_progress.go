package pump

import "gopkg.in/cheggaaa/pb.v1"

type ProgressWriter interface {
	Increment() int
	SetTotal(total int)
	Prefix(elem string)
	Finish()
}

type ProgressBarWriter struct {
	pb *pb.ProgressBar
}

var _ ProgressWriter = (*ProgressBarWriter)(nil)

func NewProgressWriter() *ProgressBarWriter {
	progressBar := pb.StartNew(0)
	progressBar.ShowElapsedTime = true
	progressBar.ShowTimeLeft = true
	progressBar.ShowSpeed = true

	return &ProgressBarWriter{pb: progressBar}
}

func (p *ProgressBarWriter) Increment() int {
	return p.pb.Increment()
}

func (p *ProgressBarWriter) SetTotal(total int) {
	p.pb.SetTotal(total)
}

func (p *ProgressBarWriter) Prefix(elem string) {
	p.pb.Prefix(elem)
}

func (p *ProgressBarWriter) Finish() {
	p.pb.Finish()
}

type NullProgressWriter struct {
}

var _ ProgressWriter = (*NullProgressWriter)(nil)

func NewNullProgressWriter() *NullProgressWriter {
	return &NullProgressWriter{}
}

func (p *NullProgressWriter) Increment() int {
	return 0
}

func (p *NullProgressWriter) SetTotal(total int) {
}

func (p *NullProgressWriter) Prefix(elem string) {
}

func (p *NullProgressWriter) Finish() {
}
