package proto

import (
	"fmt"
	"github.com/google/cayley/quad"
)

// MakeValue converts quad.Value to its protobuf representation.
func MakeValue(qv quad.Value) *Value {
	if qv == nil {
		return nil
	}
	switch v := qv.(type) {
	case quad.Raw:
		return &Value{&Value_Raw{[]byte(v)}}
	case quad.String:
		return &Value{&Value_Str{string(v)}}
	case quad.IRI:
		return &Value{&Value_Iri{string(v)}}
	case quad.BNode:
		return &Value{&Value_Bnode{string(v)}}
	case quad.TypedString:
		return &Value{&Value_TypedStr{&Value_TypedString{
			Value: string(v.Value),
			Type:  string(v.Type),
		}}}
	case quad.LangString:
		return &Value{&Value_LangStr{&Value_LangString{
			Value: string(v.Value),
			Lang:  v.Lang,
		}}}
	default:
		panic(fmt.Errorf("unsupported type: %T", qv))
	}
}

// ToNative converts protobuf Value to quad.Value.
func (m *Value) ToNative() (qv quad.Value) {
	if m == nil {
		return nil
	}
	switch v := m.Value.(type) {
	case *Value_Raw:
		return quad.Raw(v.Raw)
	case *Value_Str:
		return quad.String(v.Str)
	case *Value_Iri:
		return quad.IRI(v.Iri)
	case *Value_Bnode:
		return quad.BNode(v.Bnode)
	case *Value_TypedStr:
		return quad.TypedString{
			Value: quad.String(v.TypedStr.Value),
			Type:  quad.IRI(v.TypedStr.Type),
		}
	case *Value_LangStr:
		return quad.LangString{
			Value: quad.String(v.LangStr.Value),
			Lang:  v.LangStr.Lang,
		}
	default:
		panic(fmt.Errorf("unsupported type: %T", m.Value))
	}
}

// GetNativeValue returns the value stored in Node.
func (m *NodeData) GetNativeValue() quad.Value {
	if m == nil {
		return nil
	} else if m.Value == nil {
		if m.Name == "" {
			return nil
		}
		return quad.Raw(m.Name)
	}
	return m.Value.ToNative()
}

func (m *NodeData) Upgrade() {
	if m.Value == nil {
		m.Value = MakeValue(quad.Raw(m.Name))
		m.Name = ""
	}
}

// MakeQuad converts quad.Quad to its protobuf representation.
func MakeQuad(q quad.Quad) *Quad {
	return &Quad{
		SubjectValue:   MakeValue(q.Subject),
		PredicateValue: MakeValue(q.Predicate),
		ObjectValue:    MakeValue(q.Object),
		LabelValue:     MakeValue(q.Label),
	}
}

// ToNative converts protobuf Quad to quad.Quad.
func (m *Quad) ToNative() (q quad.Quad) {
	if m == nil {
		return
	}
	if m.SubjectValue != nil {
		q.Subject = m.SubjectValue.ToNative()
	} else if m.Subject != "" {
		q.Subject = quad.Raw(m.Subject)
	}
	if m.PredicateValue != nil {
		q.Predicate = m.PredicateValue.ToNative()
	} else if m.Predicate != "" {
		q.Predicate = quad.Raw(m.Predicate)
	}
	if m.ObjectValue != nil {
		q.Object = m.ObjectValue.ToNative()
	} else if m.Object != "" {
		q.Object = quad.Raw(m.Object)
	}
	if m.LabelValue != nil {
		q.Label = m.LabelValue.ToNative()
	} else if m.Label != "" {
		q.Label = quad.Raw(m.Label)
	}
	return
}

func (m *Quad) Upgrade() {
	if m.SubjectValue == nil {
		m.SubjectValue = MakeValue(quad.Raw(m.Subject))
		m.Subject = ""
	}
	if m.PredicateValue == nil {
		m.PredicateValue = MakeValue(quad.Raw(m.Predicate))
		m.Predicate = ""
	}
	if m.ObjectValue == nil {
		m.ObjectValue = MakeValue(quad.Raw(m.Object))
		m.Object = ""
	}
	if m.LabelValue == nil && m.Label != "" {
		m.LabelValue = MakeValue(quad.Raw(m.Label))
		m.Label = ""
	}
}
